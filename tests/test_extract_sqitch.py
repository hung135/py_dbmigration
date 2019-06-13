import sys
import unittest
 
import os, logging as log
 
 
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)
 
class Test_extract_sqitch(unittest.TestCase,Config):
      
  
    def test_extract(self):
 
        print('# In function:', sys._getframe().f_code.co_name) 
        
        import py_dbmigration.extract_db_tables_to_sqitch as sqitch_extract
        sqitch_extract.main(s=self.TEST_SCHEMA,
        o=self.dirs['sample_working_dir'])
        
if __name__ == '__main__':
    unittest.main()
