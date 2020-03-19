import os, logging as lg
import logging.handlers
#using root logger so this need to come before any other logger that may get call inside one of the imports below
##############################################

import sys
import unittest
 
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table

#from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import shutil
logging = lg.getLogger()

log_level=str(os.environ.get("LOGLEVEL", "INFO")).upper()
class Test_db_utils_postgres(unittest.TestCase,Config):
     
    LOGFORMAT=f'%(process)d, %(levelname)s,%(filename)s," \t%(message)s"'
    log_level=str(os.environ.get("LOGLEVEL", "INFO").upper())
    #logging.basicConfig(level=log_level, format=LOGFORMAT)
  
    def test_data_load_postgres_utils(self):
        print('# In function:', sys._getframe().f_code.co_name) 
        shutil.copyfile("/workspace/tests/sample_data/Contacts_Demo_200101.zip","/workspace/tests/sample_data/Contacts_Demo_200102.zip")
        shutil.copyfile("/workspace/tests/sample_data/Contacts_Demo_200101.zip","/workspace/tests/sample_data/Contacts_Demo_duplicate.zip")
         
        db=self.get_pg_database(appname=self.whoami(), loglevel=logging.level)
         
        db.execute("truncate table logging.meta_source_files")

        try:
            import py_dbmigration.data_load as data_load
            data_load.main(yamlfile='/workspace/tests/data_load.yaml',
            write_path=self.dirs['sample_working_dir'],
                        schema=self.TEST_SCHEMA, logging_mode=log_level or 'ERROR')
            
            sql="""select count(*) from logging.meta_source_files where file_process_state='OBSOLETE'"""
            count,=db.get_a_row(sql)
            self.assertTrue(int(count)>0)
            sql="""select count(*) from logging.meta_source_files where file_process_state='DUPLICATE'"""
            count,=db.get_a_row(sql)
            self.assertTrue(int(count)>0)
        except Exception as e:
            logging.exception(f'Unknown Error: {e}')
        import time
        db.__del__()
        print("sleeeping for 500")
        #time.sleep(500)
if __name__ == '__main__':

   
    # handler = logging.handlers.WatchedFileHandler(
    #     os.environ.get("LOGFILE", ".dataload_log.csv"))
    # formatter = logging.Formatter(LOGFORMAT)
    # handler.setFormatter(formatter)
    # root = lg.getLogger()
    # root.setLevel(log_level)
    # root.addHandler(handler)
    unittest.main()
