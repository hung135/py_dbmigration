import sys
import unittest

from data_file_mgnt import *
from data_file_mgnt.data_files import *
from migrate_utils import *
import db_table
import logging as log

logging = log.getLogger()
logging.setLevel(log.ERROR)


class Test_db_utils_postgres(unittest.TestCase):
    HOST = 'localhost'
    HOST = '192.168.99.100'
    DATABASE = 'postgres'
    USERID = 'postgres'
    DBTYPE = 'POSTGRES'
    DATA_SCHEMA = 'test'
    DBPASSWORD = 'docker'
    DBPORT = 5432
    SAMPLE_DATA_LINE_COUNT = 15
    SAMPLE_DATA_TOTAL_TABLES = 15  # None will get all tables
    CLEAN_PREV = True
    GENERATE_SAMPLE_DATA = True
    GENERATE_SAMPLE_DATA_W_HEADER = False

    SAMPLE_DATA_HAS_HEADER = False
    GENERATE_CRC = False
    GENERATE_FILE_ID = False
    LIMIT_ROWS = None
    START_ROW = 2
    TRUNCATE_TABLE = True

    db = db_utils.dbconn.Connection(host=HOST, userid=USERID, database=DATABASE, dbschema=DATA_SCHEMA,
                                    password=DBPASSWORD,
                                    dbtype=DBTYPE, port=DBPORT)
    dirs = {
        'sample_data_dir': "./_sample_data/",
        'sample_working_dir': "./_sample_working_dir/",
        'sample_zip_data_dir': "./_sample_zip_data/"}

    # this should run first test functions run alphabetically
    @unittest.skip("only need once")
    def test_00_init(self):
        print('# In function:', sys._getframe().f_code.co_name)
        self.db.execute('create schema if not exists {}'.format(
            self.DATA_SCHEMA))  # db.execute('create  database if not exists testing')
        tbl = self.db.get_table_list_via_query(self.DATA_SCHEMA)
        for table in tbl:
            migrate_utils.static_func.add_column(self.db, self.DATA_SCHEMA +"."+ table, 'crc', 'uuid')
            migrate_utils.static_func.add_column(self.db, self.DATA_SCHEMA +"."+ table, 'file_id', 'Integer')
        self.db.commit()
    # this should run last
    def test_zz_last(self):
        print('# In function:', sys._getframe().f_code.co_name)
        # this should run last

    def test_01_clean_previous(self):
        import commands
        print('# In function:', sys._getframe().f_code.co_name)

        if self.CLEAN_PREV is not False:
            for key, dir in self.dirs.items():
                x = (("rm -rf {}/*").format(os.path.abspath(dir)))
                print("Cleaning Directory:", x)
                commands.getoutput(x)
        else:
            print("Skip Cleaning Previous")

    def test_02_record_keeper(self):

        print('# In function:', sys._getframe().f_code.co_name)

        t = db_table.db_table_func.RecordKeeper(self.db, db_table.db_table_def.MetaSourceFiles)
        row = db_table.db_table_def.MetaSourceFiles(file_path='.', file_name='abc', file_name_data='',
                                                    file_type='ZIP', parent_file_id=0)
        t.add_record(row, commit=True)
        self.db.commit()

    def test_03_query(self):
        print '# In function:', sys._getframe().f_code.co_name
        self.assertTrue(self.db.query('select 1 from logging.meta_source_files limit 1;'))

    def test_05_upsert(self):
        sql = """insert into table_b (pk_b, b)
                select pk_a,a from table_a
                on conflict ({}) do update set b=excluded.b;"""

        self.assertTrue((static_func.generate_postgres_upsert(self.db, 'meta_source_files', 'stg', 'logging')))
        # self.db.get_primary_keys('logging.meta_source_files')

    @static_func.timer
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


    @unittest.skip("Skipping for now")
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
        self.db.commit()
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

        t = db_table.db_table_func.RecordKeeper(self.db, table_def=db_table.db_table_def.TableFilesRegex)

        records = t.get_all_records()

        # GENERATING FOI FROM DB META DATA IN LOGGING SCHEMA TABLE TableFilesRegex
        for r in records:
            assert isinstance(r, db_table.db_table_def.TableFilesRegex)

            foi_list.append(data_files.FilesOfInterest(
                file_type='CSV', table_name=str(r.table_name), file_regex=str(r.regex),
                file_delimiter=str(r.delimiter), column_list=None, schema_name=str(r.db_schema),
                has_header=self.SAMPLE_DATA_HAS_HEADER, append_file_id=self.GENERATE_FILE_ID,
                append_crc=self.GENERATE_CRC,
                limit_rows=self.LIMIT_ROWS, start_row=self.START_ROW, insert_option=self.TRUNCATE_TABLE))

        df.do_work(self.db, cleanup=False, limit_rows=None, import_type=df.IMPORT_VIA_PANDAS)

    def clean_db(self):
        self.db.execute(
            "TRUNCATE table logging.meta_source_files, logging.table_file_regex, logging.error_log, logging.load_status RESTART IDENTITY;")
        self.db.execute("""INSERT into logging.table_file_regex SELECT distinct concat(table_name,'.*.csv'),',',
                       table_schema,table_name,now(),TRUE
                       FROM information_schema.columns a
                       WHERE table_schema = '{}'""".format(self.DATA_SCHEMA))
        self.db.commit()

    def make_foi(self):
        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path=self.dirs["sample_data_dir"],
                                       parent_file_id=0)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path=self.dirs["sample_zip_data_dir"],
                                       parent_file_id=0))
        return foi_list

    @static_func.timer
    def test_07_import_data(self):
        print('# In function:', sys._getframe().f_code.co_name)

        import itertools
        logging.setLevel(log.WARN)

        #SAMPLE_DATA_HAS_HEADER = [True, False]
        CLEAN_PREV = [True, False]
        GENERATE_SAMPLE_DATA = [True, False]
        GENERATE_SAMPLE_DATA_W_HEADER = [True, False]
        GENERATE_CRC = [ False, True]
        GENERATE_FILE_ID = [  False, True]
        LIMIT_ROWS = [None, 10]
        START_ROW = [0]
        TRUNCATE_TABLE = [True]
        IMPORT_METHOD = [data_files.DataFile.IMPORT_VIA_PANDAS,data_files.DataFile.IMPORT_VIA_CLIENT_CLI]

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
            print("---------",
                  '_GENERATE_CRC', \
                  '_GENERATE_FILE_ID', \
                  '_LIMIT_ROWS', \
                  '_START_ROW', \
                  '_TRUNCATE_TABLE', \
                  '_IMPORT_METHOD', "-----------")
            print("---------",
            _GENERATE_CRC,\
            _GENERATE_FILE_ID,\
            _LIMIT_ROWS,\
            _START_ROW,\
            _TRUNCATE_TABLE,\
            _IMPORT_METHOD,"-----------")
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


if __name__ == '__main__':
    unittest.main()
