import uuid, nanotime, time, uuid, random, sys, copy

from pssolib.utils import *

# FIXME (Register) enforce a single key in every value
# FIXME (Cas) backslash the ":" separator 
# FIXME Cas and consensus not localy concurrent objects

######################################
# Base concurrent recyclable objects #
######################################

class Register():

    def __init__(self,columnFamily,initValue,key,ts=0):
        self.key = key
        self.columnFamily = columnFamily
        self.initValue = initValue
        self.ts = ts
        
    def write(self,val):
        val['ts'] = str(self.ts)
        self.columnFamily.insert(self.key,val)
        
    def read(self):
        try:
            val = self.columnFamily.get(self.key)
            if int(val['ts']) >= self.ts:
                del val['ts']
                return val
        except NotFoundException:
            pass
        return self.initValue

# FIXME no atomicity of row writing ... thanks Cassandra ! 
class Snapshot():

    def __init__(self,columnFamily,initValue,key,ts=0):
        self.key = key
        self.columnFamily = columnFamily
        self.initValue = initValue
        self.ts = ts

    def write(self,val):
        wval = dict()
        for (k,v) in val.iteritems():
            wval[k] = str(v)+":"+str(self.ts)
        # print "SNAPSHOT writing "+str(wval)
        self.columnFamily.insert(self.key,wval)

    def read(self):
        try:
            val = self.columnFamily.get(self.key)
            # print "SNAPSHOT reading "+str(val)
            # compute the pairs that are in
            rval = dict()
            for (k,v) in val.iteritems():
                if int(v.rsplit(":")[1]) >= self.ts:
                    rval[k] = v.rsplit(":")[0]
            # complete if needed
            for (k,v) in self.initValue.iteritems():
                if k not in rval:
                    rval[k] = v
            return rval
        except NotFoundException:
            pass
        return self.initValue

# cost = 4 * 4
class Splitter():

    def __init__(self,key,ts=0):
        self.pid = get_thread_ident()
        self.x = Register(Config.get().SPLITTERX,{'x':None},key,ts)
        self.y = Register(Config.get().SPLITTERY,{'y':False},key,ts)
        # print "SPLITTER ("+str(ts)+") "+str(key)

    def split(self):

        # FIXME useful ? 
        if self.x.read()['x'] != None:
            return False

        self.x.write({'x':self.pid})

        if self.y.read()['y'] != False:
            return False

        self.y.write({'y':True})
        
        if self.x.read()['x'] != self.pid:
            return False        
        
        return True

# cost = 6 * 4
class WeakAdoptCommit():

    def __init__(self,key,ts=0):
        self.splitter = Splitter(key,ts)        
        self.d = Register(Config.get().WACD,{'d':None},key,ts)
        self.c = Register(Config.get().WACC,{'c':False},key,ts)
        print "WAC ("+str(ts)+") "+str(key)

    def adoptCommit(self,u):
        d = self.d.read()['d'] 
        if d != None:
            if self.c.read()['c'] == True:
                return (d,'ADOPT')
            return (d,'COMMIT')

        if self.splitter.split()==False :
            # print "WAC splitter lost"
            self.c.write({'c':True})
            d = self.d.read()['d']
            if d != None:
                return (d,'ADOPT')
            return (u,'ADOPT')

        self.d.write({'d':u})
        if self.c.read()['c'] == True:
            return (u,'ADOPT')
        return (u,'COMMIT')

##################
# Racing objects #
##################

# Metaclass
class Racing():

    def __init__(self,key,class_name,ts=0):
        self.class_name = class_name
        self.key = key
        self.ts = ts
        self.pid = get_thread_ident()

        self.current = 0
        self.snap = Snapshot(Config.get().MAP,dict(),key,ts)

    def newinstance(self,k,ts):
        module = __import__("pssolib").types
        class_ = getattr(module, self.class_name)
        return class_(k,ts)

    # FIXME move this
    def min(self,snap):
        m = self.current
        for (k,v) in snap.iteritems():
            if int(v) < m:
                m = int(v)
        return m

    def max(self,snap):
        m = self.current
        for (k,v) in snap.iteritems():
            if int(v) > m:
                m = int(v)
        return m

class UnboundedRacing(Racing):

    def __init__(self,key,class_name,ts=0):
        Racing.__init__(self,key,class_name,ts)
        # print "RACING "+"("+str(ts)+") "+str(key)

    def enter(self):
        # print "RACING leaving "+str(self.current)
        self.snap.write({str(self.pid):str(self.current)})
        snap = self.snap.read()
        # print "RACING state of the game "+str(snap)
        m = self.max(snap)
        if self.current == m:
            self.current = m +1
        else:
            self.current = m
        # print "RACING entering "+str(self.current)
        return self.newinstance(random_uuid(str(self.key)+str(self.current)),self.ts)

class BoundedRacing(Racing):

    def __init__(self,key,class_name,ts=0):
        Racing.__init__(self,key,class_name,ts)

    def enter(self,m,rnd):
        snap = self.snap.read()
        self.current = m
        self.snap.write({str(self.pid):str(self.current)})
        return self.newinstance(random_uuid(str(self.key)+str(self.current)),rnd)

    def free(self):
        m = self.current
        snap = self.snap.read()
        # if smallest > 0 then smallest-1 is free
        smallest = self.min(snap)
        if smallest > 0:
            return smallest - 1
        # no smallest object, go for the greatest + 1
        return self.max(snap) + 1

##############################
# Complex concurrent objects #
##############################

# cost = 8 * 4
class Consensus():

    def __init__(self,key,ts=0):
        self.pid = get_thread_ident()
        self.d = Register(Config.get().CONSENSUS,{'d':None},key,ts)
        self.R = UnboundedRacing(key,"WeakAdoptCommit",ts)
        # print "CONS "+"("+str(ts)+") "+str(key)

    def propose(self,u):
        p = u
        while True:
            d = self.d.read()['d']
            if d != None:
                # print "CONS (early) "+str(d)
                return d
            r = self.R.enter().adoptCommit(p)
            # print "CONS "+str(r)
            p = r[0]
            if r[1] == 'COMMIT':                
                self.d.write({'d':p})
                return p

    def decision(self):
        return self.d.read()['d']


# cost = 9 * 4
class Cas():

    def __init__(self,key,init):
        self.key = key
        self.pid = get_thread_ident()
        self.state = init
        self.nextRound = 0

        self.R = BoundedRacing(key,"Consensus")
        self.nextLap = 0
        # print "entering (init) "+str(self.nextRound)
        self.C = self.R.enter(0,0)

    def compareandswap(self,u,v):
        while True:
            decision = self.C.decision()
            # print "["+str(decision)+"]"
            if decision != None:
                self.state = decision.rsplit(":")[0]
                self.nextLap = int(decision.rsplit(":")[2])
                self.nextRound = int(decision.rsplit(":")[3])
                # print "CAS "+str(self.nextRound-1)+"["+str(decision)+"]"
                self.C = self.R.enter(self.nextLap,self.nextRound)
            else:
                if self.state != u:
                    return False; 
                self.nextRound +=1
                decision = self.C.propose(v+":"+str(self.pid)+":"+str(self.R.free())+":"+str(self.nextRound))
                if decision.rsplit(":")[1] == str(self.pid):
                    print "CAS (True) "+str(self.nextRound-1)+" "+str(u)+" "+str(v)
                    return True                

    def get(self):
        while True:
            decision = self.C.decision()
            if decision == None:
                return self.state
            self.state = decision.rsplit(":")[0]
            self.nextLap = int(decision.rsplit(":")[2])
            self.nextRound = int(decision.rsplit(":")[3])
            # print "entering (get) "+str(self.nextRound)
            self.C = self.R.enter(self.nextLap,self.nextRound)

class Spinlock():

    def __init__(self,lockid):
        self.pid = get_thread_ident()
        self.lockid = lockid
        self.cas = Cas(self.lockid,str(0))
        
    def lock(self):
        mdelay=0
        maxdelay=32
        while self.cas.compareandswap(str(0),str(get_thread_ident())) != True:
            if mdelay==0:
                mdelay=2
            else:
                mdelay=mdelay*2 # exp backoff
            if mdelay>maxdelay:
                mdelay=maxdelay # with a cap
            sleeptime = random.expovariate(1.0/mdelay)
            time.sleep(sleeptime*0.001)
            pass
        mdelay=0
        
    def unlock(self):
         r = self.cas.compareandswap(str(get_thread_ident()),str(0))
         assert r == True

class Stack():

    def __init__(self,id):
        self.id = id
        self.head = Cas(self.id,str(0))
        self.REGISTER = Config.get().REGISTER
        
    def push(self,e):
        
        # print >> sys.stderr, "PUSH IN"
        # 1 - insert data
        k = random_uuid(e)
        self.REGISTER.insert(k,{'c':e})

        # 2 - update head
        while True:
            head = self.head.get()
            c = str(k)+":"+str(head)
            l = random_uuid(c)
            self.REGISTER.insert(l,{'c':c})
            if self.head.compareandswap(head,str(l)) == True:
                # print >> sys.stderr, "PUSH OUT"
                return
            self.REGISTER.remove(l)
                   
            
    def pop(self):
        # print >> sys.stderr, "POP IN"
        if self.empty():
            # print >> sys.stderr, "POP OUT "+"(empty)"
            return None
        while True:
            head = self.head.get()
            try:
                c = self.REGISTER.get(uuid.UUID(head))['c']
            except NotFoundException:
                # print >> sys.stderr, "POP OUT"
                return None
            if self.head.compareandswap(head,c.rsplit(":")[1]) == True:
                r = self.REGISTER.get(uuid.UUID(c.rsplit(":")[0]))['c']
                if r == "0":
                    # print >> sys.stderr, "POP OUT"
                    return None
                # print >> sys.stderr, "POP OUT"
                return r

    def empty(self):
        return self.head.get() == "0"
    
