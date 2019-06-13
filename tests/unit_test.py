import sys
import unittest

import py_dbmigration.data_file_mgnt.data_files as data_files
import py_dbmigration.migrate_utils.static_func as static_func
import py_dbmigration.db_table as db_table
import os, logging as log
runtime_pid=os.getpid()
 
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')
logging.setLevel(log.DEBUG)
 
class Test_db_utils_postgres(unittest.TestCase,Config):
     
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
 

    # this should run first test functions run alphabetically
    #@unittest.skip("only need once")
    def test_00_init(self):
        self.db=self.get_pg_database(self.whoami)
        print('# In function:', sys._getframe().f_code.co_name)
        self.db.execute('create schema if not exists {}'.format(
            self.TEST_SCHEMA))  # db.execute('create  database if not exists testing')
         
        tbl = self.db.get_all_tables()
        
        for table in tbl:
            
            if table.startswith(self.TEST_SCHEMA+'.'):
                 
                static_func.add_column(self.db, self.TEST_SCHEMA +"."+ table, 'crc', 'uuid')
                static_func.add_column(self.db, self.TEST_SCHEMA +"."+ table, 'file_id', 'Integer')
        for x in self.dirs:
            print(x)
            os.makedirs(name=self.dirs[x],exist_ok = True)
    # this should run last
    def test_zz_last(self):
        print('# In function:', sys._getframe().f_code.co_name)
        # this should run last

  

    def test_02_record_keeper(self):
        file_name=sys._getframe().f_code.co_name
        db=self.get_pg_database(appname=self.whoami())

        print('# In function:', sys._getframe().f_code.co_name)
        file_name=os.path.basename(__file__)
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles,appname=file_name)
        row = db_table.db_table_def.MetaSourceFiles(project_name=self.PROJECT_NAME, file_path='.', file_name='abc', file_name_data='',
                                                    file_type='ZIP', parent_file_id=0)
        t.add_record(row, commit=True)
        db.commit()

    def test_03_query(self):
        file_name=sys._getframe().f_code.co_name
        db=self.get_pg_database(appname=self.whoami())
        print('# In function:', sys._getframe().f_code.co_name)
        x=db.query('select 1 from logging.meta_source_files limit 1;')
         
        self.assertTrue(x)
    #@unittest.skip("Not yet")
    def test_05_upsert(self):
        file_name=sys._getframe().f_code.co_name
        db=self.get_pg_database(appname=self.whoami())
        sql = """insert into table_b (pk_b, b)
                select pk_a,a from table_a
                on conflict ({}) do update set b=excluded.b;"""

        self.assertTrue((static_func.generate_postgres_upsert(db, 'meta_source_files', 'stg', 'logging')))
        # self.db.get_primary_keys('logging.meta_source_files')

    #@static_func.timer
    def test_06_migrate_utils(self):
        file_name=sys._getframe().f_code.co_name
        db=self.get_pg_database(appname=self.whoami())
        if self.GENERATE_SAMPLE_DATA:
            # static_func.generate_data_sample(self.db,'xref_clickwrap_agreement',self.schema,'_sample_data/xref_clickwrap_agreement.csv',line_count=5)
            static_func.generate_data_sample_all_tables(db, self.TEST_SCHEMA, self.dirs['sample_data_dir'],
                                                        line_count=self.SAMPLE_DATA_LINE_COUNT,
                                                        zip_file_name=os.path.join(self.dirs['sample_zip_data_dir'],
                                                                                   'sample_data.zip'),
                                                        num_tables=self.SAMPLE_DATA_TOTAL_TABLES,
                                                        post_fix='2018.csv',
                                                        include_header=self.GENERATE_SAMPLE_DATA_W_HEADER
                                                        )


    #@unittest.skip("Skipping for now")
    def test_08_walkdir_data_file(self):
        file_name=sys._getframe().f_code.co_name
        db=self.get_pg_database(appname=self.whoami())
        print('# In function:', sys._getframe().f_code.co_name)
        # datafiles = dfm.DataFile([dfm.FilesOfInterest('account', r'^d.*.txt', '', None, self.schema, has_header=self.SAMPLE_DATA_HAS_HEADER)]
        print("Truncating Logging Tables:")

        db.execute(
            "TRUNCATE table logging.meta_source_files, logging.table_file_regex, logging.error_log, logging.load_status RESTART IDENTITY;")

        db.execute("""INSERT into logging.table_file_regex SELECT distinct concat(table_name,'.*.csv'),',',
        table_schema,table_name,now(),TRUE
        FROM information_schema.columns a
        WHERE table_schema = '{}'""".format(self.TEST_SCHEMA))
        
        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path=self.dirs["sample_data_dir"],
                                       parent_file_id=0)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path=self.dirs["sample_zip_data_dir"],
                                       parent_file_id=0))

        df = data_files.DataFile(working_path=self.dirs["sample_working_dir"], db=db, foi_list=foi_list,
                                 parent_file_id=0)
        assert isinstance(df, data_files.DataFile)
        result_set = db.query("select * from logging.meta_source_files")
        self.assertGreater(len(result_set), 0, "No files Found: Check Regex Logic")

        # t = db_table.db_table_func.RecordKeeper(self.db, table_def=db_table.db_table_def.TableFilesRegex)

        # records = t.get_all_records()

        # # GENERATING FOI FROM DB META DATA IN LOGGING SCHEMA TABLE TableFilesRegex
        # for r in records:
        #     assert isinstance(r, db_table.db_table_def.TableFilesRegex)

        #     foi_list.append(data_files.FilesOfInterest(
        #         file_type='CSV', table_name=str("tbl_1"), file_regex=str(r.regex),
        #         file_delimiter=str(r.delimiter), column_list=None, schema_name=str(r.db_schema),
        #         has_header=self.SAMPLE_DATA_HAS_HEADER, append_file_id=self.GENERATE_FILE_ID,
        #         append_crc=self.GENERATE_CRC,
        #         limit_rows=self.LIMIT_ROWS, start_row=self.START_ROW, insert_option=self.TRUNCATE_TABLE))

        df.do_work(db, cleanup=False, limit_rows=None)

    def clean_db(self):
        file_name=__file__
        db=self.get_pg_database(appname=self.whoami())
        db.execute(
            "TRUNCATE table logging.meta_source_files, logging.table_file_regex, logging.error_log, logging.load_status RESTART IDENTITY;")
        db.execute("""INSERT into logging.table_file_regex SELECT distinct concat(table_name,'.*.csv'),',',
                       table_schema,table_name,now(),TRUE
                       FROM information_schema.columns a
                       WHERE table_schema = '{}'""".format(self.TEST_SCHEMA))
        

    def make_foi(self):
        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path=self.dirs["sample_data_dir"],
                                       parent_file_id=0)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path=self.dirs["sample_zip_data_dir"],
                                       parent_file_id=0))
        return foi_list


    def test_profile_csv(self):
        print('# In function:', sys._getframe().f_code.co_name)
        #static_func.profile_csv(full_file_path="/Users/hnguyen/PycharmProjects/py_dbmigration/_sample_data/test_city2018.csv")
        static_func.profile_csv_directory("/Users/hnguyen/PycharmProjects/py_dbmigration/_sample_data")


    def test_check_iii(self):
        print('# In function:', sys._getframe().f_code.co_name)
        #static_func.check_pii(self.db)
if __name__ == '__main__':
    unittest.main()
