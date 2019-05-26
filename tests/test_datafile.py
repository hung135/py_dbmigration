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

    def test_00_init(self):
        print('# In function:', sys._getframe().f_code.co_name) 
        for x in self.dirs:
            os.makedirs(name=self.dirs[x],exist_ok = True)
    
    #@unittest.skip("Skipping for now")
    def test_08_walkdir_data_file(self):
        print('# In function:', sys._getframe().f_code.co_name)
        # datafiles = dfm.DataFile([dfm.FilesOfInterest('account', r'^d.*.txt', '', None, self.schema, has_header=self.SAMPLE_DATA_HAS_HEADER)]
        print("Truncating Logging Tables:")

        self.db.execute(
            "TRUNCATE table logging.meta_source_files, logging.table_file_regex, logging.error_log, logging.load_status RESTART IDENTITY;")
 
        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path=self.dirs["sample_data_dir"],
                                       parent_file_id=0,project_name=PROJECT_NAME)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path=self.dirs["sample_zip_data_dir"],
                                       parent_file_id=0,project_name=PROJECT_NAME))

        df = data_files.DataFile(working_path=self.dirs["sample_working_dir"], db=self.db, foi_list=foi_list,
                                 parent_file_id=0)
        df.init_db()
         
        assert isinstance(df, data_files.DataFile)
        #result_set = self.db.query("select * from logging.meta_source_files")
        #self.assertGreater(len(result_set), 0, "No files Found: Check Regex Logic")

        df.do_work(self.db, cleanup=False, limit_rows=None)
 
if __name__ == '__main__':
    unittest.main()
