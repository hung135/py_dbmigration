import sys
import unittest
 
from py_dbmigration.migrate_utils import static_func
 
import logging as log
import os
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import yaml

logging = log.getLogger()
logging.setLevel(log.ERROR)
 
class Test_db_utils_postgres(unittest.TestCase,Config):
     
   
  
    def test_inject_data_runtime(self):
        from py_dbmigration.data_file_mgnt.utils import inject_frame_work_data as ij
        yamlfile='/workspace/tests/data_load.yaml'
        foi=None
        with open(yamlfile,'r') as f:
            foi = yaml.load(f)
        print(foi)

        
        self.assertTrue(foi is not None)
        
if __name__ == '__main__':
    unittest.main()
