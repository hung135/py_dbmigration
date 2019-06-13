import sys
import unittest

 
from py_dbmigration.migrate_utils import static_func
from config_parent import Config
import os, logging as log
from py_dbutils.rdbms import postgres as db_utils
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)

 
class Test_db_utils_postgres(unittest.TestCase,Config):
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
    def test_print_create_table(self):
        db=self.get_pg_database(appname=self.whoami())

        print(static_func.print_create_table(db,self.dirs["sample_working_dir"],targetschema='logging'))
  
    def test_sql_to_excel(self):
        db=self.get_pg_database(appname=self.whoami())
        static_func.sql_to_excel(db,'select * from logging.meta_source_files',os.path.join(self.dirs["sample_working_dir"],'test.xls'))
if __name__ == '__main__':
    unittest.main()
