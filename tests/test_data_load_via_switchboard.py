import sys
import unittest

#from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table

from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import os
import logging as lg
#from pydbwrap.pydbwrap import DbWrap 

logging = lg.getLogger()
logging.level = lg.INFO

class Test_db_utils_postgres(unittest.TestCase,Config):
  
      
    YAML_PATH='/workspace/tests/data_load_pg_bulkcopy_switchboard.yaml'
 
     
    def test_01_data_load_pg_bulkcopy_switchboard(self):
        schema=self.get_yaml_schemas(self.YAML_PATH,logging.level)
         
        db=self.get_pg_database(appname=self.whoami(),loglevel=logging.level)
         


        db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name,self.YAML_PATH) 
        try:
            import py_dbmigration.data_load as data_load
            data_load.main(yamlfile=self.YAML_PATH,logging_mode=logging.level)
            pass             
        except Exception as e:
            logging.exception(e)
    
if __name__ == '__main__':
    unittest.main()
