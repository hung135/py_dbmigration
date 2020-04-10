import sys
import unittest 
import py_dbmigration.db_table.pid_worker as PidWorker
from  py_dbutils.rdbms import postgres as db_utils
 
import os, logging as lg

logging=lg.getLogger(__file__)
 
from config_parent import Config
#import boto3





class Test(unittest.TestCase,Config):
  
   
 
    def test_init(self):
        db=self.get_pg_database(appname=self.whoami(), loglevel=logging.level)
        pid = PidWorker.PidManager(db,'unit-test','logging','table1',True)
        pid.getwork()
       
        #pid.row.file_id=555
        pid.checkin('Completed Test Init','ERROR','test error')
        pid.commit()
 
        print(pid)
        #y = pw.PidManager(db,'unit-test2','logging','table1')
        

if __name__ == '__main__':
    unittest.main()
