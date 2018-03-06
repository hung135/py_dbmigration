import unittest

import db_utils.dbconn as db_utils
import data_file_mgnt.data_files as dfm
from data_file_mgnt.data_files import *

import db_table
import sys


class Test_db_utils(unittest.TestCase):
    #host = '192.168.99.100'
    database = 'postgres'
    dbtype = 'POSTGRES'
    host = 'localhost'
    db = db_utils.Connection(host=host,
                             userid='postgres',
                             database=database,
                             dbschema='logging',
                             dbtype=dbtype)

    def test_01_init(self):
        print '# In function:', sys._getframe().f_code.co_name
        self.db.execute('create schema if not exists logging')
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
        self.db.execute("drop schema logging cascade")
        self.db.execute("drop schema stg cascade")
        self.db.commit()


if __name__ == '__main__':
    unittest.main()
