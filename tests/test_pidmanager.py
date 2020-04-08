import sys
import unittest 
import py_dbmigration.db_table.pid_worker as pw
from  py_dbutils.rdbms import postgres as db_utils
 
import os, logging as lg

logging=lg.getLogger(__file__)
 
from config_parent import Config
#import boto3





class Test(unittest.TestCase,Config):
  
   
 
    def test_init(self):
        db=self.get_pg_database(appname=self.whoami(), loglevel=logging.level)
        x = pw.PidManager(db,'unit-test')


if __name__ == '__main__':
    unittest.main()
