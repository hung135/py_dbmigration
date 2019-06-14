import sys
import unittest

 
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
 
import pprint
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import os, logging

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')


 
class Test_db_utils_postgres(unittest.TestCase,Config):

     
  
    def test_data_load(self): 
        file_name=os.path.basename(__file__)
        db=self.get_pg_database(appname=self.whoami())
        db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name) 
        try:
            import py_dbmigration.data_load as data_load
            data_load.main(yamlfile='/workspace/tests/data_load_msaccess.yaml',
                        write_path=self.dirs['sample_working_dir'],
                        schema=self.TEST_SCHEMA)
            rs,_=db.query("""select database_table from logging.meta_source_files
                                    where database_table is not NULL""")
            for row in rs:
                print("Table Name: ",row[0])
                pprint.pprint(db.query("select * from {} limit 1".format(row[0])))
        except:
            pass
        
if __name__ == '__main__':
    unittest.main()
