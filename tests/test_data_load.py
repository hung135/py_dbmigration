import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import logging as log
import os
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shutil

logging = log.getLogger()
logging.setLevel(log.ERROR)
 
class Test_db_utils_postgres(unittest.TestCase,Config):
     
   
  
    def test_data_load(self):
        shutil.copyfile("/workspace/tests/sample_data/Contacts_Demo_200101.zip","/workspace/tests/sample_data/Contacts_Demo_200102.zip")
        db=self.get_pg_database()
         
        db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name) 
        import py_dbmigration.data_load as data_load
        data_load.main(yamlfile='/workspace/tests/data_load.yaml',
        write_path=self.dirs['sample_working_dir'],
                    schema=self.TEST_SCHEMA, logging_mode='ERROR')
        
if __name__ == '__main__':
    unittest.main()