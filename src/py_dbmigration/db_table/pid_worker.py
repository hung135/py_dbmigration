from .db_table_def import PidWorker, MetaSourceFiles
from .db_table_func import RecordKeeper
import py_dbutils.rdbms.postgres as PGDB
from sqlalchemy.sql import func
import logging as lg 
import socket
import os
 
class PidManager(object):
    
    file_id = None
    host = None
    logging = None
    pid = None
    row = None
    on_exit_deregister=True
    task_status=None
    current_task=None
    has_error = False
    def _singleton(self):
        if not (self.pid):
            self.pid=os.getpid()
            self.host=socket.gethostname()
            self.logging=lg.getLogger('PidManager')
           

        else:
            self.logging.exception('Singleton Already Initiated')

        return
    def __init__(self,db:PGDB,project_name,schema,table_name,on_exit_deregister=True):



        self._singleton()  
        self.db=db
        self.project_name=project_name
        self.schema=schema
        self.table_name=table_name
        self.on_exit_deregister=on_exit_deregister
        self.register()
    def checkin(self,current_task:str,task_status:str,detail:str=None):

        self.current_task=current_task
        self.task_status=task_status
        if task_status=='ERROR':
            self.has_error=True

        self.logging.info("Checking in")  
        if detail and not self.has_error:
            self.detail=detail
            self.row.detail=self.detail
        self.row.current_task=self.current_task
        self.row.task_status=self.task_status
        self.row.last_checkin=func.now()
        self.commit()
    def commit(self):
        self.table.add_record(self.row)
    def getwork(self,file_id:int=None):
 
        if not file_id:
            self.row.file_id=file_id
            self.table.add_record(self.row)
        return True
    def register(self):
        self.current_task='REGISTERED'
        table_def=PidWorker(self.schema,self.table_name).table_def
        self.table = RecordKeeper(self.db,table_def ,'PidManager')
        
        self.row = self.table.get_record(table_def.host == self.host,table_def.pid==self.pid,obj=table_def)
        
        if not self.row:
            self.logging.info("Row Not found Inserting")    
            self.row = table_def(pid=self.pid, host=self.host)
            self.row.current_task=self.current_task
            self.table.add_record(self.row,commit=True)
             

    
    def deregister(self):
        if self.on_exit_deregister and not self.has_error:
            self.table.delete_record(self.row)
        else:
            self.row.current_task='DEREGISTERED'
    def __str__(self): 

        string_result={
            'project_name':self.project_name,
            'pid': self.pid,
            'host':self.host,
            'current_task':self.current_task,
            'task_status': self.task_status 

        }
         
        return str(string_result)
        

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        
        return self.__str__()
    def __del__(self):
        
        self.deregister()




