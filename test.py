import unittest

from data_file_mgnt import *
from db_utils import *
from migrate_utils import *
from data_file_mgnt.data_files import *
from db_table import *
import sys


class Test_db_utils_postgres(unittest.TestCase):
    host = 'localhost'
    host = '192.168.99.100'
    database = 'postgres'
    userid = 'postgres'
    dbtype = 'POSTGRES'
    schema = 'prey'
    password = 'docker'
    port = 5432
    db = db_utils.dbconn.Connection(host=host, userid=userid, database=database, dbschema=schema, password=password,
                                    dbtype=dbtype, port=port)

    # this should run first test functions run alphabetically
    def test_00_init(self):
        print '# In function:', sys._getframe().f_code.co_name
        self.db.execute('create schema if not exists {}'.format(
            self.schema))  # db.execute('create  database if not exists testing')

    # this should run last
    def test_zz_last(self):
        print '# In function:', sys._getframe().f_code.co_name
        # this should run last

    def test_02_record_keeper(self):
        print '# In function:', sys._getframe().f_code.co_name

        t = db_table.RecordKeeper(self.db)
        row = db_table.MetaSourceFiles(file_path='.', file_name='abc', file_name_data='',
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
        #self.db.get_primary_keys('logging.meta_source_files')

    @static_func.timer
    def test_06_migrate_utils(self):
        import shutil
        #shutil.rmtree('./_sample_data/*')
        #static_func.generate_data_sample(self.db,'xref_clickwrap_agreement',self.schema,'_sample_data/xref_clickwrap_agreement.csv',line_count=5)
        static_func.generate_data_sample_all_tables(self.db, self.schema, '_sample_data/', line_count=30,
                                                   zip_file_name='./_sample_zip_data/sample_data.zip')

    @static_func.timer
    def test_07_import_data(self):
        print '# In function:', sys._getframe().f_code.co_name

    def test_08_walkdir_data_file(self):
        print '# In function:', sys._getframe().f_code.co_name
        # datafiles = dfm.DataFile([dfm.FilesOfInterest('account', r'^d.*.txt', '', None, self.schema, has_header=True)]
        print(
            "Truncating meta_source_files",
            self.db.execute("TRUNCATE table logging.meta_source_files RESTART IDENTITY;"))

        # This is how we store the files we are looking for List of FileOfInterest
        foi_list = [
            data_files.FilesOfInterest('CSV', file_regex=r".*\.csv", file_path="./_sample_data/", parent_file_id=0)]
        foi_list.append(
            data_files.FilesOfInterest('ZIP', file_regex=r".*\.zip", file_path="./_sample_zip_data/", parent_file_id=0))

        foi_list.append(data_files.FilesOfInterest(
            file_type='CSV', table_name='xref_clickwrap_agreement', file_regex=r'.*xref_clickwrap_agreement.*.csv', file_delimiter=',',
            column_list=None, schema_name=self.schema, has_header=True,append_file_id=False,append_crc=False))
        foi_list.append(data_files.FilesOfInterest(
            file_type='CSV', table_name='cr_property', file_regex=r'.*cr_property.*.csv', file_delimiter=',',
            column_list=None, schema_name=self.schema, has_header=True))

        df = data_files.DataFile(working_path="./_sample_working_dir/", db=self.db, foi_list=foi_list, parent_file_id=0)
        assert isinstance(df, data_files.DataFile)
        result_set = self.db.query("select * from logging.meta_source_files")
        self.assertGreater(len(result_set), 0, "No files Found: Check Regex Logic")

        #df.do_work(self.db, cleanup=False, limit_rows=None, import_type='Pandas')

        # uz=data_files.FilesOfInterest('CSV', file_regex=r".*\.zip", file_path="./_sample_working_dir/", parent_file_id=0)
        # df.walk_dir(uz)


if __name__ == '__main__':
    unittest.main()
