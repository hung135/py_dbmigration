import unittest

import db_utils


class TestDBConn(unittest.TestCase):
    def test_init(self):
        print("Testing Began DB connection PostGres:")
        db = db_utils.Connection(host='localhost',userid='postgres', dbschema='stg', dbtype='POSTGRES')
        print(db.query('select 1'))


        print("Testing End")

        #self.assertEqual('s',1)
if __name__ == '__main__':
    unittest.main()