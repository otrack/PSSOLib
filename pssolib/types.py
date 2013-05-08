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

    def adoptCommit(self,u):

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
            try:
                u = self.WAC.get(self.key,columns=['d'])['d']
                try:
                    self.WAC.get(self.key,columns=['c'])
                except NotFoundException:
                    return (u,'COMMIT')
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
        self.R = NaturalRacing()

    def propose(self,u):
        while True:
            try:
                return self.CONSENSUS.get(self.key,columns=['d'])['d']
            except NotFoundException:
                pass
            r = WeakAdoptCommit(uuid_add(self.key,self.R.enter(self.pid))).adoptCommit(u)
            u = r[0]
            if r[1] == 'COMMIT':
                self.CONSENSUS.insert(self.key,{'d':u})

class Cas():

    def __init__(self,key,init):
        self.key = key
        self.CAS = Config.get().CAS
        self.compareandswap(None,init)
 
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
        self.cas = Cas(self.lockid,str(0))
        
    def lock(self):
        while self.cas.compareandswap(str(0),str(get_thread_ident())) != True:
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

#############################
# Racing
##############################

class Racing():

    def enter(pid):
        raise Error("Uncorrect racing definition")

class NaturalRacing(Racing):

    def __init__(self):
        self.i = 0

    def enter(self,pid):
        self.i = self.i + 1
        return self.i

