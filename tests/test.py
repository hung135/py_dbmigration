import sys
import unittest

from py_dbmigration.data_file_mgnt import *
from py_dbmigration.data_file_mgnt.data_files import *
from py_dbmigration.migrate_utils import *
import py_dbmigration.db_table
from  py_dbutils.rdbms import postgres as db_utils
import os, logging as log
import boto3
from config_parent import Config
#import boto3

logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)


class Test_db_utils_postgres(unittest.TestCase,Config):
  
   
 
    def test_01(self):
        file_name=sys._getframe().f_code.co_name
        db=self.get_pg_database(appname=file_name)
        print("test function",self)
        rs=self.db.query("select * from information_schema.tables")
        for row in rs:
            print(row)
    def test_02(self):
   
        ec2client = boto3.client('ec2')
        response = ec2client.describe_instances()
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                # This sample print will output entire Dictionary object
                print(instance)
                # This will print will output the value of the Dictionary key 'InstanceId'
                print(instance["InstanceId"])


if __name__ == '__main__':
    unittest.main()
