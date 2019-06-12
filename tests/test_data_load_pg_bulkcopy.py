import sys
import unittest

#from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import logging as log
import os
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

logging = log.getLogger()
logging.setLevel(log.DEBUG)
 
class Test_db_utils_postgres(unittest.TestCase,Config):
  
      
    
    def test_data_load_pg_bulkcopy(self):
        file_name=__file__
        db=self.get_pg_database(appname=self.whoami())
         
        db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name) 
        try:
            import py_dbmigration.data_load as data_load
            data_load.main(yamlfile='/workspace/tests/data_load_pg_bulkcopy.yaml',logging_mode='info')
        except:
            pass
if __name__ == '__main__':
    unittest.main()
