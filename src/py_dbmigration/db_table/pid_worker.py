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
    def __init__(self,db:PGDB,project_name,schema,table_name):
        self._singleton()  
        self.db=db
        self.project_name=project_name
        self.schema=schema
        self.table_name=table_name
        self.register()
    def commit(self):
        self.table.add_record(self.row)
    def getwork(self):
        import random
        i=0
        while i<1000000:
            file_id=random.randrange(10000) 
            self.row.file_id=file_id
            i+=1

            self.table.add_record(self.row)
        return True
    def register(self):
         
        table_def=PidWorker(self.schema,self.table_name).table_def
        self.table = RecordKeeper(self.db,table_def ,'PidManager')
        
        self.row = self.table.get_record(table_def.host == self.host,table_def.pid==self.pid,obj=table_def)
      
        if not self.row:
                
            self.row = table_def(pid=self.pid, host=self.host)
    
            self.table.add_record(self.row,commit=True)
            print("record added")

    
    def deregister(self):
        self.table.delete_record(self.row)
    def __str__(self): 
        return self.pid

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        
        return self.__str__()
    def __del__(self):
        self.deregister()




