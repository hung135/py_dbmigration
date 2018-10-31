import sys
import unittest

from data_file_mgnt import *
from data_file_mgnt.data_files import *
from migrate_utils import *
import db_table
import logging as log

logging = log.getLogger()
logging.setLevel(log.DEBUG)


class Test_db_utils_postgres(unittest.TestCase):
    HOST = 'localhost'
    HOST = 'localhost'
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
    GENERATE_SAMPLE_DATA_W_HEADER = True

    SAMPLE_DATA_HAS_HEADER = False
    GENERATE_CRC = False
    GENERATE_FILE_ID = False
    LIMIT_ROWS = None
    START_ROW = 2
    TRUNCATE_TABLE = [True, False, None]

    db = db_utils.dbconn.Connection(host=HOST, userid=USERID, database=DATABASE, dbschema=DATA_SCHEMA,
                                    password=DBPASSWORD,
                                    dbtype=DBTYPE, port=DBPORT)

    from parameterized import parameterized

    # SAMPLE_DATA_HAS_HEADER,GENERATE_FILE_ID,GENERATE_CRC,LIMIT_ROWS,START_ROW,TRUNCATE_TABLE,IMPORT_VIA_PANDAS

    def test_zzsequence(self):
        import itertools

        SAMPLE_DATA_HAS_HEADER = [True, False]
        CLEAN_PREV = [True, False]
        GENERATE_SAMPLE_DATA = [True, False]
        GENERATE_SAMPLE_DATA_W_HEADER = [True, False]
        SAMPLE_DATA_HAS_HEADER = [True, False]
        GENERATE_CRC = [True, False]
        GENERATE_FILE_ID = [True, False]
        LIMIT_ROWS = [None, 10]
        START_ROW = [0, 3]
        TRUNCATE_TABLE = [True, False]

        print("call funct(")
        x = itertools.product(
            SAMPLE_DATA_HAS_HEADER,
            CLEAN_PREV,
            GENERATE_SAMPLE_DATA,
            GENERATE_SAMPLE_DATA_W_HEADER,
            SAMPLE_DATA_HAS_HEADER,
            GENERATE_CRC,
            GENERATE_FILE_ID,
            LIMIT_ROWS,
            START_ROW,
            TRUNCATE_TABLE)
        for a in x:
            z,b,c,d,e,f,g,h,i,j=a
            print(a,type(a))
            print(z,b,c,d,e,f,g,h,i,j)


        # self.assertEqual(a,b)


if __name__ == '__main__':
    unittest.main()
