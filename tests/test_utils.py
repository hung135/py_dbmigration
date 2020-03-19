import sys
import unittest
 
from py_dbmigration.migrate_utils import static_func
 
import os, logging as lg

logging=lg.getLogger()
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import yaml
import pprint as pp



 
class Test_db_utils_postgres(unittest.TestCase,Config):
     
   
  
    def test_inject_data_runtime(self):
        from py_dbmigration.data_file_mgnt.utils import inject_frame_work_data as ifwd
        from py_dbmigration.data_file_mgnt.utils import recurse_replace_yaml as rry
        from py_dbmigration.data_file_mgnt.utils import pre_process_yaml as ppy
        yamlfile='/workspace/tests/data_load.yaml'
        pre_processed_yaml=ppy(yamlfile)
        
        
        pp.pprint(pre_processed_yaml)
        #pp.pprint(rry(foi),foi[0])
        #print(rry(foi,foi))
        

        
        #self.assertTrue(foi is not None)
        
if __name__ == '__main__':
    unittest.main()
