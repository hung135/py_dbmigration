# import csv, pandas,        sqlalchemy, os
import os
import db_utils
import data_file_mgnt as dfm
import migrate_utils as migu
import inflection as inflect


import logging

logging.basicConfig(level='DEBUG')
file_path = os.environ['RAWFILEPATH']
writable_path = os.environ['WORKINGPATH']
host = os.environ['MSSQLHOST']
pghost = os.environ['PGHOST']
"""
    Creating a Database connection object
"""
dbmssql = db_utils.Connection(dbschema='dbo',database='ComplianceToolkit', dbtype='MSSQL',host="wdcsqlaw02",commit=False)
dbPOSTGRES = db_utils.Connection(dbschema='stg', database='compliance', dbtype='POSTGRES', host=pghost, commit=False)

#migu.print_create_table(db, folder="/home/cfpb/nguyenhu/compliancemssql/", targetschema="enforce")
migu.print_create_table(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancestgsql/", targetschema="stg")
#migu.print_create_table_upsert(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancesql/", targetschema="enforce")
#migu.print_postgres_table(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancesql/", targetschema="enforce")
#migu.appdend_to_readme(dbPOSTGRES, folder="/home/cfpb/nguyenhu/compliancesql/", targetschema="enforce")
