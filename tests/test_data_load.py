import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table

from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shutil
import os, logging as log
runtime_pid=os.getpid()
logging = log.getLogger(f'P:{os.getpid()}{os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)
 
class Test_db_utils_postgres(unittest.TestCase,Config):
     
   
  
    def test_data_load_postgres_utils(self):
        shutil.copyfile("/workspace/tests/sample_data/Contacts_Demo_200101.zip","/workspace/tests/sample_data/Contacts_Demo_200102.zip")
        shutil.copyfile("/workspace/tests/sample_data/Contacts_Demo_200101.zip","/workspace/tests/sample_data/Contacts_Demo_duplicate.zip")
         
        db=self.get_pg_database(appname=self.whoami())
         
        db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name) 
        try:
            import py_dbmigration.data_load as data_load
            data_load.main(yamlfile='/workspace/tests/data_load.yaml',
            write_path=self.dirs['sample_working_dir'],
                        schema=self.TEST_SCHEMA, logging_mode='ERROR')
            
            sql="""select count(*) from logging.meta_source_files where file_process_state='OBSOLETE'"""
            count,=db.get_a_row(sql)
            self.assertTrue(int(count)>0)
            sql="""select count(*) from logging.meta_source_files where file_process_state='DUPLICATE'"""
            count,=db.get_a_row(sql)
            self.assertTrue(int(count)>0)
        except Exception as e:
            logging.error(f'Unknown Error: {e}')
        import time
        db.__del__()
        print("sleeeping for 500")
        #time.sleep(500)
if __name__ == '__main__':
    unittest.main()
