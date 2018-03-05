import unittest

import db_utils.dbconn as db_utils
import data_file_mgnt.data_files as dfm

class Test_db_utils(unittest.TestCase):

    def test_init(self):
        print("Testing Began DB connection PostGres:")

        db = db_utils.Connection(host='localhost',userid='postgres', dbschema='stg', dbtype='POSTGRES')

        db.execute('create schema logging')
        print(db.query('select 1'))
        print("Testing End")

        #self.assertEqual('s',1)


class Test_data_file_mgnt(unittest.TestCase):

    def test_init(self):
        db = db_utils.Connection(host='localhost', userid='postgres', dbschema='stg', dbtype='POSTGRES')
        print("Testing Begin:")

        datafiles = [dfm.DestinationDB('account', r'^d.*.txt', '', None, 'logging', has_header=True)]
        print(db.query("select * from logging.meta_source_files"))

        print("Testing End")

        # self.assertEqual('s',1)


if __name__ == '__main__':
    unittest.main()