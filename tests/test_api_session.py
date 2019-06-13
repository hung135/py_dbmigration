import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import os, logging as log
import pprint
from py_dbutils.rdbms import postgres as db_utils
import requests 
import json, yaml
from config_parent import Config
from bs4 import BeautifulSoup
import time
from prettytable import PrettyTable

logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)

TEST_SCHEMA = 'test'
LOGGING_SCHEMA = 'logging'
PROJECT_NAME= 'test_project'
class TestSessionAPI(unittest.TestCase,Config):
    
    def print_table(self,db,sql):
        proj , meta=(db.query(sql))
        cols=[]
        for col in meta:
            cols.append(col.name)
        t = PrettyTable(cols)
        for r in proj:
            t.add_row(r)
        print(t) 
    

    def test_sqlalchemy_session(self):
        conn_name='TestSQLALCHEMY'
        db=self.get_pg_database(conn_name)
         
        #db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name) 

        file_name=os.path.basename(__file__)+"init_db"
        t= db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles,appname=file_name)
        t.close()
        db.execute('truncate table logging.meta_source_files')
        


        sql="""select application_name,state
                ,query 
            from pg_stat_activity
             where application_name not like 'pgAdmin%' and 
             application_name !='' """
        self.print_table(db,sql)
        db.close()
        
        print(self.whoami())
        #sleep 500 slo we can manually check
        print("Closing EVERYTHING...Sleeping go check PGADMIN Dashboard")
        #time.sleep(500)

if __name__ == '__main__':
    unittest.main()
