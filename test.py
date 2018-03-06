import unittest

import db_utils.dbconn as db_utils
import data_file_mgnt.data_files as dfm
from data_file_mgnt.data_files import *

import db_table
import sys




class Test_db_utils_postgres(unittest.TestCase):
    host = 'localhost'
    #host = '192.168.99.100'
    database = 'postgres'
    userid = 'postgres'
    dbtype = 'POSTGRES'
    schema = 'logging'
    password = ''
    port = 5432
    db = db_utils.Connection(host=host,
                             userid=userid,
                             database=database,
                             dbschema='logging',
                             password=password,
                             dbtype=dbtype,
                             port=port)

    def test_01_init(self):
        print '# In function:', sys._getframe().f_code.co_name
        self.db.execute('create schema if not exists {}'.format(self.schema))
        # db.execute('create  database if not exists testing')

    def test_02_record_keeper(self):
        print '# In function:', sys._getframe().f_code.co_name

        t = db_table.RecordKeeper(self.db)
        row = db_table.MetaSourceFiles(file_path='.', file_name='abc',
                                       file_name_data='', file_type=DestinationDB.file_type,
                                       parent_file_id=DestinationDB.parent_file_id)
        t.add_record(row, commit=True)
        self.db.commit()

    def test_03_query(self):
        print '# In function:', sys._getframe().f_code.co_name
        print("-------", self.db.query('select count(*) from logging.meta_source_files;'))

    def test_04_data_file(self):
        print '# In function:', sys._getframe().f_code.co_name
        datafiles = [dfm.DestinationDB('account', r'^d.*.txt', '', None, 'logging', has_header=True)]
        print("Files Found:", self.db.query("select * from logging.meta_source_files"))

    def test_zz_clean(self):
        print '# In function:', sys._getframe().f_code.co_name
        print("Dropping schema:")
        self.db.execute("drop schema {} ".format(self.schema))

class Test_db_utils(unittest.TestCase):
    host = 'localhost'
    #host = '192.168.99.100'
    database = 'mysql'
    userid = 'root'
    dbtype = 'MYSQL'
    schema = 'logging'
    password = 'test'
    port = 3306
    db = db_utils.Connection(host=host,
                             userid=userid,
                             database=database,
                             dbschema='logging',
                             password=password,
                             dbtype=dbtype,
                             port=port)

    def test_01_init(self):
        print '# In function:', sys._getframe().f_code.co_name
        self.db.execute('create schema if not exists {}'.format(self.schema))
        # db.execute('create  database if not exists testing')

    def test_02_record_keeper(self):
        print '# In function:', sys._getframe().f_code.co_name

        t = db_table.RecordKeeper(self.db)
        row = db_table.MetaSourceFiles(file_path='.', file_name='abc',
                                       file_name_data='', file_type=DestinationDB.file_type,
                                       parent_file_id=DestinationDB.parent_file_id)
        t.add_record(row, commit=True)
        self.db.commit()

    def test_03_query(self):
        print '# In function:', sys._getframe().f_code.co_name
        print("-------", self.db.query('select count(*) from logging.meta_source_files;'))

    def test_04_data_file(self):
        print '# In function:', sys._getframe().f_code.co_name
        datafiles = [dfm.DestinationDB('account', r'^d.*.txt', '', None, 'logging', has_header=True)]
        print("Files Found:", self.db.query("select * from logging.meta_source_files"))

    def test_zz_clean(self):
        print '# In function:', sys._getframe().f_code.co_name
        print("Dropping schema:")
        self.db.execute("drop schema {} ".format(self.schema))


if __name__ == '__main__':
    unittest.main()
