import sys
import unittest

 
from py_dbmigration.migrate_utils import static_func
 
import logging as log
import os
from py_dbutils.rdbms import postgres as db_utils
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

logging = log.getLogger()
logging.setLevel(log.ERROR)

 
class Test_db_utils_postgres(unittest.TestCase):
    def funct(self,loop=100000000):
            y=0
            for i in range(loop):
                y+=1
            print(y)
    def test_00_timer(self):
        print('# In function:', sys._getframe().f_code.co_name)
        print("Timer",static_func.timer(self.funct))
    def test_00_dumpparams(self):
        print('# In function:', sys._getframe().f_code.co_name)
        print("dump params",static_func.dump_params(self.funct))
    def test_zz_last(self):
        print('# In function:', sys._getframe().f_code.co_name)
        # this should run last

  
 
if __name__ == '__main__':
    unittest.main()
