import sys
import unittest
import csv
from py_dbmigration.data_file_mgnt import data_files
from py_dbmigration.migrate_utils import static_func
import py_dbmigration.db_table as db_table
import pandas
from py_dbutils.rdbms import postgres as db_utils
from config_parent import Config
#sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
import hashlib
import os, logging as lg

logging=lg.getLogger()


class TestGlobalHashList(unittest.TestCase,Config):
    
 
    def read_in_hash(self,file_abs_path):
        with open(file_abs_path, mode='a') as f:
            f.write('aaaa,bbb\n')
        with open(file_abs_path, mode='r') as infile:
            reader = csv.reader(infile)
            hashdict = {rows[0]:rows[1] for rows in reader}
        return hashdict

    def test_00_init(self):
        print('# In function:', sys._getframe().f_code.co_name) 
        print(self.read_in_hash('.filehash.csv'))
            

    
    #@unittest.skip("Skipping for now")
    def test_query_to_dict(self):
        db=self.get_pg_database(loglevel=logging.level)
        df=pandas.read_sql('select file_name,file_path,crc from logging.meta_source_files where crc is not NULL',db.connect_SqlAlchemy())
        df['file_hash'] = df.apply(lambda row: hashlib.md5(os.path.join(row.file_path,row.file_name).encode('utf8')).hexdigest(), axis=1)
        df.to_csv('.filehash.csv', columns=['file_hash','crc'],index=False)
if __name__ == '__main__':
    unittest.main()
