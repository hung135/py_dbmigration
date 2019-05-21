import sys
import unittest

import data_file_mgnt.data_files as data_files
import migrate_utils.static_func as static_func
import db_table 
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

    # this should run first test functions run alphabetically
    #@unittest.skip("only need once")
    def test_00_init(self):
        print('# In function:', sys._getframe().f_code.co_name)
        self.db.execute('create schema if not exists {}'.format(
            self.DATA_SCHEMA))  # db.execute('create  database if not exists testing')
         
        tbl = self.db.get_all_tables()
        
        for table in tbl:
            
            if table.startswith(self.DATA_SCHEMA+'.'):
                 
                static_func.add_column(self.db, self.DATA_SCHEMA +"."+ table, 'crc', 'uuid')
                static_func.add_column(self.db, self.DATA_SCHEMA +"."+ table, 'file_id', 'Integer')
        for x in self.dirs:
            print(x)
            os.makedirs(name=self.dirs[x],exist_ok = True)
    # this should run last
    def test_zz_last(self):
        print('# In function:', sys._getframe().f_code.co_name)
        # this should run last

  

    def test_02_record_keeper(self):

        print('# In function:', sys._getframe().f_code.co_name)

        t = db_table.db_table_func.RecordKeeper(self.db, db_table.db_table_def.MetaSourceFiles)
        row = db_table.db_table_def.MetaSourceFiles(project_name=PROJECT_NAME, file_path='.', file_name='abc', file_name_data='',
                                                    file_type='ZIP', parent_file_id=0)
        t.add_record(row, commit=True)
        self.db.commit()

    def test_03_query(self):
        print('# In function:', sys._getframe().f_code.co_name)
        x=self.db.query('select 1 from logging.meta_source_files limit 1;')
        print(x)
        self.assertTrue(x)
    @unittest.skip("Not yet")
    def test_05_upsert(self):
        sql = """insert into table_b (pk_b, b)
                select pk_a,a from table_a
                on conflict ({}) do update set b=excluded.b;"""

        self.assertTrue((static_func.generate_postgres_upsert(self.db, 'meta_source_files', 'stg', 'logging')))
        # self.db.get_primary_keys('logging.meta_source_files')

    #@static_func.timer
    def test_06_migrate_utils(self):
        if self.GENERATE_SAMPLE_DATA:
            # static_func.generate_data_sample(self.db,'xref_clickwrap_agreement',self.schema,'_sample_data/xref_clickwrap_agreement.csv',line_count=5)
            static_func.generate_data_sample_all_tables(self.db, self.DATA_SCHEMA, self.dirs['sample_data_dir'],
                                                        line_count=self.SAMPLE_DATA_LINE_COUNT,
                                                        zip_file_name=os.path.join(self.dirs['sample_zip_data_dir'],
                                                                                   'sample_data.zip'),
                                                        num_tables=self.SAMPLE_DATA_TOTAL_TABLES,
                                                        post_fix='2018.csv',
                                                        include_header=self.GENERATE_SAMPLE_DATA_W_HEADER
                                                        )


    #@unittest.skip("Skipping for now")
    def test_08_walkdir_data_file(self):
        print('# In function:', sys._getframe().f_code.co_name)
        # datafiles = dfm.DataFile([dfm.FilesOfInterest('account', r'^d.*.txt', '', None, self.schema, has_header=self.SAMPLE_DATA_HAS_HEADER)]
        print("Truncating Logging Tables:")

        self.db.execute(
            "TRUNCATE table logging.meta_source_files, logging.table_file_regex, logging.error_log, logging.load_status RESTART IDENTITY;")

        self.db.execute("""INSERT into logging.table_file_regex SELECT distinct concat(table_name,'.*.csv'),',',
        table_schema,table_name,now(),TRUE
        FROM information_schema.columns a
        WHERE table_schema = '{}'""".format(self.DATA_SCHEMA))
        
        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path=self.dirs["sample_data_dir"],
                                       parent_file_id=0)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path=self.dirs["sample_zip_data_dir"],
                                       parent_file_id=0))

        df = data_files.DataFile(working_path=self.dirs["sample_working_dir"], db=self.db, foi_list=foi_list,
                                 parent_file_id=0)
        assert isinstance(df, data_files.DataFile)
        result_set = self.db.query("select * from logging.meta_source_files")
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

        df.do_work(self.db, cleanup=False, limit_rows=None)

    def clean_db(self):
        self.db.execute(
            "TRUNCATE table logging.meta_source_files, logging.table_file_regex, logging.error_log, logging.load_status RESTART IDENTITY;")
        self.db.execute("""INSERT into logging.table_file_regex SELECT distinct concat(table_name,'.*.csv'),',',
                       table_schema,table_name,now(),TRUE
                       FROM information_schema.columns a
                       WHERE table_schema = '{}'""".format(self.DATA_SCHEMA))
        

    def make_foi(self):
        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path=self.dirs["sample_data_dir"],
                                       parent_file_id=0)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path=self.dirs["sample_zip_data_dir"],
                                       parent_file_id=0))
        return foi_list

    @unittest.skip("skipping")
    def test_07_import_data(self):
        print('# In function:', sys._getframe().f_code.co_name)

        import itertools
        logging.setLevel(log.WARN)

        #SAMPLE_DATA_HAS_HEADER = [True, False]
        CLEAN_PREV = [True, False]
        GENERATE_SAMPLE_DATA = [True, False]
        GENERATE_SAMPLE_DATA_W_HEADER = [True]
        GENERATE_CRC = [ False, True]
        GENERATE_FILE_ID = [  False, True]
        LIMIT_ROWS = [None, 10]
        START_ROW = [0]
        TRUNCATE_TABLE = [True, False]
        IMPORT_METHOD = [data_files.DataFile.IMPORT_VIA_PANDAS] #,data_files.DataFile.IMPORT_VIA_CLIENT_CLI]

        x = itertools.product(
            #SAMPLE_DATA_HAS_HEADER,

            GENERATE_CRC,
            GENERATE_FILE_ID,
            LIMIT_ROWS,
            START_ROW,
            TRUNCATE_TABLE,
            IMPORT_METHOD)
        for params in x:
            #_SAMPLE_DATA_HAS_HEADER,\
            _GENERATE_CRC,\
            _GENERATE_FILE_ID,\
            _LIMIT_ROWS,\
            _START_ROW,\
            _TRUNCATE_TABLE,\
            _IMPORT_METHOD= params
            print('# In function:', sys._getframe().f_code.co_name)

            static_func.print_padded(20,
                  '_GENERATE_CRC', \
                  '_GENERATE_FILE_ID', \
                  '_LIMIT_ROWS', \
                  '_START_ROW', \
                  '_TRUNCATE_TABLE', \
                  '_IMPORT_METHOD')
            static_func.print_padded(20,
            _GENERATE_CRC,\
            _GENERATE_FILE_ID,\
            _LIMIT_ROWS,\
            _START_ROW,\
            _TRUNCATE_TABLE,\
            _IMPORT_METHOD)
            self.clean_db()
            foi_list=self.make_foi()
            df = data_files.DataFile(working_path=self.dirs["sample_working_dir"], db=self.db, foi_list=foi_list,
                                     parent_file_id=0)
            assert isinstance(df, data_files.DataFile)
            t = db_table.db_table_func.RecordKeeper(self.db, table_def=db_table.db_table_def.TableFilesRegex)
            records = t.get_all_records()
            t.session.close()
            for r in records:
                assert isinstance(r, db_table.db_table_def.TableFilesRegex)
                foi_list.append(data_files.FilesOfInterest(
                    file_type='CSV', table_name=str(r.table_name), file_regex=str(r.regex),
                    file_delimiter=str(r.delimiter), column_list=None, schema_name=str(r.db_schema),
                    has_header=self.GENERATE_SAMPLE_DATA_W_HEADER, append_file_id=_GENERATE_FILE_ID,
                    append_crc=_GENERATE_CRC,
                    limit_rows=_LIMIT_ROWS, start_row=_START_ROW, insert_option=_TRUNCATE_TABLE))
            df.do_work(self.db, cleanup=False, limit_rows=None, import_type=_IMPORT_METHOD)



    def test_profile_csv(self):
        print('# In function:', sys._getframe().f_code.co_name)
        #static_func.profile_csv(full_file_path="/Users/hnguyen/PycharmProjects/py_dbmigration/_sample_data/test_city2018.csv")
        static_func.profile_csv_directory("/Users/hnguyen/PycharmProjects/py_dbmigration/_sample_data")


    def test_check_iii(self):
        print('# In function:', sys._getframe().f_code.co_name)
        #static_func.check_pii(self.db)
if __name__ == '__main__':
    unittest.main()
