import uuid, nanotime, time, uuid, random, sys

from pssolib.utils import *

# FIXME define a concurrent map  object 
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

# cost = 4
class Splitter():

    def __init__(self,key,ts=0):
        self.pid = get_thread_ident()
        self.x = Register(Config.get().SPLITTERX,{'x':None},key,ts)
        self.y = Register(Config.get().SPLITTERY,{'y':False},key,ts)
        # print str(key)+":"+str(ts)

    def split(self):

        # if self.x.read()['x'] != None:
        #     return False

        self.x.write({'x':self.pid})

        if self.y.read()['y'] != False:
            return False

        self.y.write({'y':True})
        
        if self.x.read()['x'] != self.pid:
            return False        
        
        return True

# cost = 6
class WeakAdoptCommit():

    def __init__(self,key,ts=0):
        self.splitter = Splitter(key,ts)        
        self.d = Register(Config.get().WACD,{'d':None},key,ts)
        self.c = Register(Config.get().WACC,{'c':False},key,ts)
        # print "WAC("+str(ts)+")"+str(key)

    def adoptCommit(self,u):

        # d = self.d.read()['d']
        # if d != None:
        #     return (d,'ADOPT')
 
        if self.splitter.split()==False :
            self.c.write({'c':True})
            d = self.d.read()['d']
        else:
            d = u
            self.d.write({'d':u})
        
        c = self.c.read()['c']
        if c == True:
            return (d,'ADOPT')
        return (d,'COMMIT')

##################
# Racing objects #
##################

# FIXME move pid to the constructor.

# Metaclass
class Racing():

    def __init__(self,key,class_name,ts=0):
        self.class_name = class_name
        self.key = key
        self.ts = ts
        self.pid = get_thread_ident()

        self.next = 0
        self.snap = Register(Config.get().MAP,dict(),key,ts)

    def newinstance(self,k,ts):
        module = __import__("pssolib").types
        class_ = getattr(module, self.class_name)
        return class_(k,ts)

    # FIXME move this
    def min(self,snap):
        m = self.next
        for (k,v) in snap.iteritems():
            if int(v) < m:
                m = int(v)
        return m

    def max(self,snap):
        m = self.next
        for (k,v) in snap.iteritems():
            if int(v) > m:
                m = int(v)
        return m

class UnboundedRacing(Racing):

    def __init__(self,key,class_name,ts=0):
        Racing.__init__(self,key,class_name,ts)

    def enter(self):
        snap = self.snap.read()
        self.next = self.max(snap)+1
        self.snap.write({str(self.pid):str(self.next)})
        return self.newinstance(random_uuid(str(uuid_add(self.key,self.next))),self.ts)

class BoundedRacing(Racing):

    def __init__(self,key,class_name,ts=0):
        Racing.__init__(self,key,class_name,ts)

    def enter(self,m,rnd):
        snap = self.snap.read()
        self.next = m
        self.snap.write({str(self.pid):str(self.next)})
        return self.newinstance(random_uuid(str(uuid_add(self.key,self.next))),rnd)

    def free(self):
        m = self.next
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

# cost = 8
class Consensus():

    def __init__(self,key,ts=0):
        self.pid = get_thread_ident()
        self.d = Register(Config.get().CONSENSUS,{'d':None},key,ts)
        self.R = UnboundedRacing(key,"WeakAdoptCommit",ts)

    def propose(self,u):
        while True:
            d = self.d.read()['d']
            if d != None:
                return d
            r = self.R.enter().adoptCommit(u)
            if r[1] == 'COMMIT':
                self.d.write({'d':r[0]})
                return r[0]

    def decision(self):
        return self.d.read()['d']


# cost = 9
class Cas():

    def __init__(self,key,init):
        self.key = key
        self.pid = get_thread_ident()
        self.state = init
        self.nextRound = 0

        self.R = BoundedRacing(key,"Consensus")
        self.nextLap = 0
        self.C = self.R.enter(0,0)

    def compareandswap(self,u,v):
        while True:
            decision = self.C.decision()
            if decision != None:
                self.state = decision.rsplit(":")[0]
                self.nextLap = int(decision.rsplit(":")[2])
                self.nextRound = int(decision.rsplit(":")[3])
                self.C = self.R.enter(self.nextLap,self.nextRound)
            else:
                if self.state != u:
                    return False; 
                self.nextRound +=1
                decision = self.C.propose(v+":"+str(self.pid)+":"+str(self.R.free())+":"+str(self.nextRound))
                if decision.rsplit(":")[1] != str(self.pid):
                    return False
                return True                

    def get(self):
        while True:
            decision = self.C.decision()
            if decision == None:
                return self.state
            self.state = decision.rsplit(":")[0]
            self.nextLap = int(decision.rsplit(":")[2])
            self.nextRound = int(decision.rsplit(":")[3])
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
    


##############
# Deprecated #
##############

class PseudoRacing(Racing):

    def __init__(self,key,class_name):
        Racing.__init__(self,class_name)
        self.class_name = class_name
        self.key = key
        self.i = 0

    def enter(self,pid):
        self.i = self.i + 1        
        return self.newinstance(uuid_add(self.key,self.i))

