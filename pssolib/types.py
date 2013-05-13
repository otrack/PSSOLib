import uuid, nanotime, time, md5, uuid, random

from pssolib.utils import *

class Splitter():

    def __init__(self,key):
        self.key = key
        self.pid = get_thread_ident()
        self.SPLITTER = Config.get().SPLITTER

    def split(self):
        self.SPLITTER.insert(self.key,{'x':self.pid})
        try:
            self.SPLITTER.get(self.key,columns=['y'])
            return False
        except NotFoundException:
            pass

        self.SPLITTER.insert(self.key,{'y':True})
        x = self.SPLITTER.get(self.key,columns=['x'])

        if x['x']!=self.pid:
            return False

        return True

class WeakAdoptCommit():

    def __init__(self,key):
        self.key = key
        self.WAC = Config.get().WAC

    def adoptCommit(self,u,k):

        s = Splitter(self.key)
        isWin = s.split()

        if isWin==True :
            self.WAC.insert(self.key,{'d':u})
            try:
                self.WAC.get(self.key,columns=['c'])
            except NotFoundException:
                return (u,'COMMIT')

        else : 

            # Check that the result does not exist first.
            for i in range(0,k):
                try:
                    v = self.WAC.get(self.key,columns=['d'])['d']
                    try:
                        self.WAC.get(self.key,columns=['c'])
                    except NotFoundException:
                        return (v,'COMMIT')
                    return (v,'ADOPT')
                except NotFoundException:
                    pass

            self.WAC.insert(self.key,{'c':1})
            try:
                u = self.WAC.get(self.key,columns=['d'])['d']
            except NotFoundException:
                pass

        return (u,'ADOPT')
        
class Consensus():

    def __init__(self,key):
        self.key = key
        self.pid = get_thread_ident()
        self.CONSENSUS = Config.get().CONSENSUS
        # FIXME
        m = md5.new()
        m.update(str(key))
        self.R = NaturalRacing(uuid.UUID(m.hexdigest()),"WeakAdoptCommit")

    def propose(self,u):
        # print "proposing "+u+" in "+ "Consensus#"+str(self.key)
        k=0
        while True:

            k=k+1
            r = self.R.enter(self.pid).adoptCommit(u,k)
            u = r[0]
            if r[1] == 'COMMIT':
                self.CONSENSUS.insert(self.key,{'d':u})
                print str(self.key)+" decision "+u
                return u

            try:
                d=self.CONSENSUS.get(self.key,columns=['d'])['d']
                return d
            except NotFoundException:
                pass

    def decision(self):
        try:
            d = self.CONSENSUS.get(self.key,columns=['d'])['d']
            print str(self.key)+" decision "+d
            return d
        except NotFoundException:
            return None

class Cas():

    def __init__(self,key,init):
        self.key = key
        self.pid = get_thread_ident()
        self.R = NaturalRacing(key,"Consensus")
        self.C = None
        self.last = [init,str(self.pid)]
 
    def compareandswap(self,u,v):

        while True:

            if self.C == None:
                self.C = self.R.enter(self.pid)

            decision = self.C.decision()
            
            if decision == None:

                if self.last[0] != u:
                    # print "failed with "+str(self.last)+" "+u+";"+v
                    return False;
            
                self.last = self.C.propose(v+":"+str(self.pid)).rsplit(":",1)
            
                if self.last[1] == str(self.pid):
                    return True
                
                if self.last[0] != u:
                    return False
        
            self.last = decision.rsplit(":",1)
        
            self.C = self.R.enter(self.pid)

    # def get(self):
    #     C = self.R.enter(self.pid)
    #     if C.decision() != None:
    #         self.last = C.decision().rsplit(":",1)
    #     return self.last[0]

class Spinlock():

    def __init__(self,lockid):
        self.pid = get_thread_ident()
        self.lockid = lockid
        self.cas = Cas(self.lockid,str(0))
        
    def lock(self):
        mdelay=0
        maxdelay=0.032
        while self.cas.compareandswap(str(0),str(get_thread_ident())) != True:
            if mdelay==0:
                mdelay=1
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

# No concurrent write 
class Map():

    def __init__(self,key):
        self.MAP = Config.get().MAP
        self.key = key
        
    def get(self,index):        
        return self.MAP.get(self.key,columns=[index])
    
    def put(self,index,value):
        self.MAP.insert(self.key,{index:value})

    # linearizable call cause every column is linearizable 
    def toOrderedDict(self): 
        try:
            return self.MAP.get(self.key,column_count=cass_max_col)
        except NotFoundException:
            return dict()

    def keyset(self):
        return self.toOrderedDict().keyset()

    def valueset(self):
        return self.toOrderedDict().valueset()

#############################
# Racing
##############################

class Racing():

    def __init__(self,class_name):
        self.class_name = class_name

    def enter(self,pid):
        raise Error("Uncorrect racing definition")

    def newinstance(self,k):
        module = __import__("pssolib").types
        class_ = getattr(module, self.class_name)
        return class_(k)

class PseudoRacing(Racing):

    def __init__(self,key,class_name):
        Racing.__init__(self,class_name)
        self.class_name = class_name
        self.key = key
        self.i = 0

    def enter(self,pid):
        self.i = self.i + 1        
        return self.newinstance(uuid_add(self.key,self.i))

class NaturalRacing(Racing):

    def __init__(self,key,class_name):
        Racing.__init__(self,class_name)
        self.last = None
        self.key = key
        self.M = Map(self.key)

    def enter(self,pid):

        if self.last != None:
            self.M.put(str(pid),str(self.last))
            m = self.last
        else:
            m = 0

        for (k,v) in self.M.toOrderedDict().iteritems():
            if int(v) > m:
                m = int(v)

        if m == self.last:
            m = m + 1

        self.last = m

        print "entering "+self.class_name+"#"+str(uuid_add(self.key,m))
        return self.newinstance(uuid_add(self.key,m))
    
