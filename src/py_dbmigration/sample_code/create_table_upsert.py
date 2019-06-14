# import csv, pandas,        sqlalchemy, os
 
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
 

import os, logging

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')

file_path = os.environ['RAWFILEPATH']
writable_path = os.environ['WORKINGPATH']
host = os.environ['MSSQLHOST']
pghost = os.environ['PGHOST']
"""
    Creating a Database connection object
"""
dbmssql = db_utils.DB(schema='dbo',database='ComplianceToolkit', dbtype='MSSQL',host="wdcsqlaw02",commit=False)
dbPOSTGRES = db_utils.DB(schema='stg', database='compliance', dbtype='POSTGRES', host=pghost, commit=False)

#migu.print_create_table(db, folder="/home/cfpb/nguyenhu/compliancemssql/", targetschema="enforce")
migu.print_create_table(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancestgsql/", targetschema="stg")
#migu.print_create_table_upsert(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancesql/", targetschema="enforce")
#migu.print_postgres_table(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancesql/", targetschema="enforce")
#migu.appdend_to_readme(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancesql/", targetschema="enforce")
