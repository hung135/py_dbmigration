import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import logging as log
import os
from py_dbutils.rdbms import postgres as db_utils
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

logging = log.getLogger()
logging.setLevel(log.ERROR)

TEST_SCHEMA = 'test'
LOGGING_SCHEMA = 'logging'
PROJECT_NAME= 'test_project'
class Test_db_utils_postgres(unittest.TestCase):
    HOST = 'pgdb'
    DATABASE = 'postgres'
    USERID = 'docker'
    DBTYPE = 'POSTGRES'
    DATA_SCHEMA = 'prey'
    
    DBPASSWORD = 'docker'
    DBPORT = 5432
    SAMPLE_DATA_LINE_COUNT = 1500
    SAMPLE_DATA_TOTAL_TABLES = 8  # None will get all tables
    CLEAN_PREV = False
    GENERATE_SAMPLE_DATA = False
    GENERATE_SAMPLE_DATA_W_HEADER = True

    SAMPLE_DATA_HAS_HEADER = False
    GENERATE_CRC = False
    GENERATE_FILE_ID = False
    LIMIT_ROWS = None
    START_ROW = 2
    TRUNCATE_TABLE = True

    db = db_utils.DB(host=HOST, userid=USERID, dbname=DATABASE, schema=DATA_SCHEMA,
                                    pwd=DBPASSWORD,  port=DBPORT)
    db.execute("create schema {}".format(LOGGING_SCHEMA))
    db.execute("create schema {}".format(TEST_SCHEMA))
    db.execute("create schema {}".format(DATA_SCHEMA))
    dirs = {
        'sample_data_dir': "./_sample_data/",
        'sample_working_dir': "./_sample_working_dir/",
        'sample_zip_data_dir': "./_sample_zip_data/"}

    def test_data_load(self):
        self.db.execute("truncate table logging.meta_source_files")
        print('# In function:', sys._getframe().f_code.co_name) 
        import py_dbmigration.data_load as data_load
        data_load.main(yamlfile='/workspace/tests/data_load.yaml')
 
if __name__ == '__main__':
    unittest.main()
