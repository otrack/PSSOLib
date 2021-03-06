####################################
# Cassandra
####################################

cass_max_col = 2147483647


####################################
# Config 
####################################

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.cassandra.ttypes import NotFoundException
from pycassa.types import *
from pycassa.system_manager import *
from pycassa.cassandra.ttypes import InvalidRequestException, ConsistencyLevel
from random import choice
from types import *

class Config():

    _instance = None

    @classmethod
    def get(cls):
        return cls._instance

    @classmethod
    def create(cls,parser,init):
        cls._instance = cls(parser,init)

    def createComlumnFamily(cfname,cfdef):
        cf = ColumnFamily(self.POOL, cfname,)
        cf.read_consistency_level = ConsistencyLevel.QUORUM
        cf.write_consistency_level = ConsistencyLevel.QUORUM
        cf.key_validation_class = AsciiType()
        cf.column_name_class = AsciiType()
        for k,v in cfdef.iteritems():
            if type(v) is IntType:
                cf.column_validators[k] = IntegerType()
            elif type(v) is BooleanType:
                cf.column_validators[k] = BooleanType()
            elif type(v) is  StringType:
                cf.column_validators[k] = AsciiType()
        return cf

    def __init__(self,parser,init):

        # 1 - read the config parser

        self.SERVERS = parser.get("main","servers").split(",")
        self.SYSM = SystemManager(choice(self.SERVERS))
        self.KEYSPACE = parser.get("main","keyspace")        
        
        # 2 - get or create the keyspace and the column families for each object

        if init or ((self.KEYSPACE in self.SYSM.list_keyspaces())!=True) :
            try:
                self.SYSM.drop_keyspace(self.KEYSPACE)
            except:
                pass

            if 'localhost' in self.SERVERS:
                print "Replication factor = 1"
                replication_factor = '1'
            else:
                print "Replication factor = 3"
                replication_factor = '3'                

            self.SYSM.create_keyspace(self.KEYSPACE, SIMPLE_STRATEGY, {'replication_factor': replication_factor},durable_writes=True)
            self.SYSM.create_column_family(self.KEYSPACE, 'register')
            self.SYSM.create_column_family(self.KEYSPACE, 'wregister')
            self.SYSM.create_column_family(self.KEYSPACE, 'map')
            self.SYSM.create_column_family(self.KEYSPACE, 'splitterx')
            self.SYSM.create_column_family(self.KEYSPACE, 'splittery')
            self.SYSM.create_column_family(self.KEYSPACE, 'wacc')
            self.SYSM.create_column_family(self.KEYSPACE, 'wacd')

        self.POOL = ConnectionPool(self.KEYSPACE, server_list=self.SERVERS) 

        self.SPLITTERX = ColumnFamily(self.POOL, 'splitterx')
        self.SPLITTERX.read_consistency_level = ConsistencyLevel.QUORUM
        self.SPLITTERX.write_consistency_level = ConsistencyLevel.QUORUM
        self.SPLITTERX.key_validation_class = LexicalUUIDType()
        self.SPLITTERX.column_name_class = AsciiType()
        self.SPLITTERX.column_validators['x'] = IntegerType() # PID

        self.SPLITTERY = ColumnFamily(self.POOL, 'splittery')
        self.SPLITTERY.read_consistency_level = ConsistencyLevel.QUORUM
        self.SPLITTERY.write_consistency_level = ConsistencyLevel.QUORUM
        self.SPLITTERY.key_validation_class = LexicalUUIDType()
        self.SPLITTERY.column_name_class = AsciiType()
        self.SPLITTERY.column_validators['y'] = BooleanType() # boolean

        self.GrafariusD = ColumnFamily(self.POOL, 'wacd')
        self.GrafariusD.read_consistency_level = ConsistencyLevel.QUORUM
        self.GrafariusD.write_consistency_level = ConsistencyLevel.QUORUM
        self.GrafariusD.key_validation_class = LexicalUUIDType()
        self.GrafariusD.column_name_class = AsciiType() 

        self.GrafariusC = ColumnFamily(self.POOL, 'wacc')
        self.GrafariusC.read_consistency_level = ConsistencyLevel.QUORUM
        self.GrafariusC.write_consistency_level = ConsistencyLevel.QUORUM
        self.GrafariusC.key_validation_class = LexicalUUIDType()
        self.GrafariusC.column_name_class = AsciiType() 
        self.GrafariusC.column_validators['c'] = BooleanType() # Conflict flag

        self.MAP = ColumnFamily(self.POOL, 'map')
        self.MAP.read_consistency_level = ConsistencyLevel.QUORUM
        self.MAP.write_consistency_level = ConsistencyLevel.QUORUM
        self.MAP.key_validation_class = LexicalUUIDType()
        self.MAP.column_name_class = AsciiType()

        self.REGISTER = ColumnFamily(self.POOL, 'register',)
        self.REGISTER.read_consistency_level = ConsistencyLevel.QUORUM
        self.REGISTER.write_consistency_level = ConsistencyLevel.QUORUM
        self.REGISTER.key_validation_class = LexicalUUIDType()
        self.REGISTER.column_name_class = AsciiType()

        self.WREGISTER = ColumnFamily(self.POOL, 'wregister',)
        self.WREGISTER.read_consistency_level = ConsistencyLevel.ONE
        self.WREGISTER.write_consistency_level = ConsistencyLevel.QUORUM
        self.WREGISTER.key_validation_class = LexicalUUIDType()
        self.WREGISTER.column_name_class = AsciiType()
        
#############################
# UUID 
##############################

import binascii, string
import uuid, md5

# Use snowflacke for generating UUID ?

# FIXME

def random_uuid(s):
    md = md5.new()
    md.update(s)
    return uuid.UUID(md.hexdigest())


# This function takes as argument an hex and increments it modulo its lenght.
# The result is a pair where the first element is the incremented element, 
# and the second one is the carry.
def hex_add(h,i):
    l=len(h)
    if h=="f"*l:
        return [1,"0"*l]
    else:
        # FIXME find a cleaner solution
        return [0,string.replace(string.replace(hex(int(h,16)+i),"0x",""),"L","").zfill(l)]

# This function adds i to uuid u
def uuid_add(u,i):
    r=str(u)
    # FIXME find a cleaner solution 
    #       _and_ do not change the value of the M^th byte.
    once=True
    for p in  str(u).split("-")[::-1]:
        if once :
            once=False
            continue
        q=hex_add(p,i)
        r=string.replace(r,p,q[1])
        if q[0]==0:
            return uuid.UUID(r)
    return uuid.UUID(r)

# This function takes as argument a UUID (v1) and increment the nanoseconds 
# value by 1 . For instance, 550e8400-e29b-41d4-a716-446655440000
# beciles 550e840ef-e29b-41d4-a717-446655440000.
def uuid_incr(u):
    return uuid_add(u,1)

#############################
# Threads
##############################

import multiprocessing

# FIXME return a system-wide unique id
#       use injjection from N*N to N
def get_thread_ident():
    return multiprocessing.current_process().pid        

#############################
# Files
##############################

import os,fuse

class Filestats(fuse.Stat):
    def __init__(self):
        self.st_mode = 0
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 0
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 0
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0

def check_filename_len(path):
    for p in path.split('/'):
        if len(p) > 255:
            raise NameTooLongException

def user_in_group(uid,gid):    
    try:
        if pwd.getpwuid(uid).pw_gid == gid:
            return True
    except KeyError:
        pass

    try:
        for user in grp.getgrgid(gid).gr_mem:
            if uid == pwd.getpwnam(user).pw_uid:
                return True
    except KeyError:
        pass

    return False
    
def get_path_components(path):
    paths = [ path ]
    while path != '/':
        path = os.path.dirname(path)
        paths.insert(0,path)
    try:
        end = paths.pop()
    except IndexError:
        end = ''

    try:
        notend = paths.pop()
    except IndexError:
        notend = ''
    
    return (end, notend, paths)

def replace(s,buf,p):
    ns = ""
    n = len(s)
    bn = len(buf)
    if n <= p:
        ns = s + ('\0' * (p-n)) + buf
    else:
        ns = s[0:p] + buf + s[p+bn:]
    return

class NameTooLongException(Exception):
    pass

class EnoentException(Exception):
    pass

class EnotdirException(Exception):
    pass

class EaccessException(Exception):
    pass

class EexistException(Exception):
    pass

