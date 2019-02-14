import sys
import unittest

from data_file_mgnt import *
from data_file_mgnt.data_files import *
from migrate_utils import *
import db_table
import logging as log
#import boto3

logging = log.getLogger()
logging.setLevel(log.DEBUG)


class Test_db_utils_postgres(unittest.TestCase):
    HOST = 'localhost'
    HOST = 'localhost'
    DATABASE = 'postgres'
    USERID = 'postgres'
    DBTYPE = 'POSTGRES'
    DATA_SCHEMA = 'public'
    DBPASSWORD = 'docker'
    DBPORT = 5432
  

    db = db_utils.dbconn.Connection(host=HOST, userid=USERID, database=DATABASE, dbschema=DATA_SCHEMA,
                                    password=DBPASSWORD,
                                    dbtype=DBTYPE, port=DBPORT)

  
 
    def test_01(self):
        print("test function",self)
        rs=self.db.query("select * from information_schema.tables")
        for row in rs:
            print(row)
    def test_02(self):
   
        ec2client = boto.client('ec2')
        response = ec2client.describe_instances()
        for reservation in response["Reservations"]:
            for instance in reservation["Instances"]:
                # This sample print will output entire Dictionary object
                print(instance)
                # This will print will output the value of the Dictionary key 'InstanceId'
                print(instance["InstanceId"])


if __name__ == '__main__':
    unittest.main()
