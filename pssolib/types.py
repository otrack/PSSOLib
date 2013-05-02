import uuid, nanotime, time

from pssolib.utils import *

class Splitter():

    def __init__(self,key):
        self.key = key
        self.pid = get_thread_ident()
        self.SPLITTER = Config.get().SPLITTER

    def split(self):
        self.SPLITTER.insert(self.key,{'x':self.pid})
        try:
            y = self.SPLITTER.get(self.key,columns=['y'])
            if y!=self.pid:
                return False
        except NotFoundException:
            pass

        self.SPLITTER.insert(self.key,{'y':self.pid})
        x = self.SPLITTER.get(self.key,columns=['x'])
        if x['x']!=self.pid:
            return False
        return True

class Consensus():

    def __init__(self,key):
        self.key = key
        self.CONSENSUS = Config.get().CONSENSUS

    def propose(self,u):
        s = Splitter(self.key)
        isWin = s.split()
        # Check that the result does not exist yet.
        try:
            u = self.CONSENSUS.get(self.key,columns=['v'])['v']
            try:
                self.CONSENSUS.get(self.key,columns=['b'])
            except NotFoundException:
                return u
        except NotFoundException:
            pass

        if isWin==True :
            # print "I win "+str(s.key)
            self.CONSENSUS.insert(self.key,{'v':u})
            # print "Value added"
            try:
                self.CONSENSUS.get(self.key,columns=['b'])
                # print "Conflict seen"
            except NotFoundException:
                # print "No conflict"
                return u
        else :
            # Again, check that the result does not exist yet.
            try:
                u = self.CONSENSUS.get(self.key,columns=['v'])['v']
                try:
                    self.CONSENSUS.get(self.key,columns=['b'])
                except NotFoundException:
                    return u
            except NotFoundException:
                pass
            # print "I lose "+str(s.key)
            self.CONSENSUS.insert(self.key,{'b':1})
            # print "Conflict added"
            try:
                u = self.CONSENSUS.get(self.key,columns=['v'])['v']
                # print "Value seen"
            except NotFoundException:
                pass
                # print "No Value"
        nested=Consensus(uuid_incr(self.key))
        return nested.propose(u)

class Cas():

    def __init__(self,key):
        self.key = key
        self.CAS = Config.get().CAS

    def compareandswap(self,u,v):
        try:
            #FIXME
            # columns = self.CAS.get(self.key, column_count=100000)
            # largest=max(columns.iterkeys())
            # if u==columns[largest]:
            c = self.CAS.get(self.key, column_count=1)
            (lkey,lval) = c.popitem()
            if u == lval:
                return self.doConsensusAndWrite(v,lkey+1)
            return False
        except NotFoundException:
            pass
        if u == None:
            return self.doConsensusAndWrite(v,1)
        else:
            return False

    def get(self):
        try:
            return self.CAS.get(self.key, column_count=1).popitem()[1]
        except:
            return None
            
    def doConsensusAndWrite(self,v,l):
        k=uuid.uuid5(self.key,str(l))
        c=Consensus(k)
        # incorrect, we must propose also our ID !!
        proposed = v
        # print "START  "+str(l)
        decided = c.propose(proposed)
        # print "OVER "+str(l)+" : "+str(decided) +" "+str(proposed)
        self.CAS.insert(self.key,{l:decided})
        if decided == proposed:
            return True
        return False

class Spinlock():

    def __init__(self,lockid):
        self.pid = get_thread_ident()
        self.lockid = lockid
        self.cas = Cas(self.lockid)
        self.cas.compareandswap(None,str(0))
        
    def lock(self):
        # mdelay=0
        # maxdelay=0.010
        while self.cas.compareandswap(str(0),str(get_thread_ident())) != True:
            # if mdelay==0:
            #     mdelay=0.001
            # mdelay=mdelay+0.001 # lin backoff
            # if mdelay>maxdelay:
            #     mdelay=maxdelay # with a cap
            # time.sleep(mdelay)
            pass
        print str(nanotime.now())+" LOCKED " + str(get_thread_ident())
        
    def unlock(self):
         r = self.cas.compareandswap(str(get_thread_ident()),str(0))
         # assert r == True 
         print str(nanotime.now())+" UNLOCKED " + str(get_thread_ident())

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
        over = False
        previous = None
        while over != True:
            current = self.MAP.get(self,column_count=cass_max_col)
            over = (previous == current)
            previous = current
        return current

    def keyset(self):
        return self.toOrderedDict().keyset()

    def valueset(self):
        return self.toOrderedDict().valueset()


