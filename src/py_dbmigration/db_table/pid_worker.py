from .db_table_def import PidWorker, MetaSourceFiles
from .db_table_func import RecordKeeper
import py_dbutils.rdbms.postgres as PGDB
import logging as lg 
import socket
import os

class PidManager(object):
    
    file_id = None
    host = None
    logging = None
    pid = None
    row = None
    def _singleton(self):
        if not (self.pid):
            self.pid=os.getpid()
            self.host=socket.gethostname()
            self.logging=lg.getLogger('PidManager')
        else:
            self.logging.exception('Singleton Already Initiated')

        return
    def __init__(self,db:PGDB,project_name):
        self._singleton()  
        self.register(db,project_name)

    def getwork(self):
        return 1
    def register(self,db,project_name):
        
        self.table = RecordKeeper(db, PidWorker,'PidManager')
        
        self.row = self.table.get_record(PidWorker.pid == self.pid)
        print("---------",type(self.row))
        if not self.row:
            self.row = PidWorker(pid=self.pid, host=self.host)
    
            self.table.add_record(self.row,commit=True)


            
        
    
    def deregister(self):
        pass
    def __str__(self): 
        return self.pid

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        
        return self.__str__()
    def __del__(self):
        self.deregister()




