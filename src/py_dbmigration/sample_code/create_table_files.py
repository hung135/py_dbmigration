# import csv, pandas,        sqlalchemy, os
import os
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import Status, import_status
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
] 


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
dbPOSTGRES = db_utils.Connection(dbschema='stg',database='compliance', dbtype='POSTGRES',host=pghost,commit=False)
 
table_list=[]

 
table_list.append('account_interest_view')
table_list.append('account_review_view')
table_list.append('account_stmt_view')
table_list.append('account_view')
table_list.append('client_add_warehouse_export')

table_list.append('client')

table_list.append('client_user_warehouse_export')

table_list.append('cr_disclosure_view')

table_list.append('fees_view')
table_list.append('loan_view')

table_list.append('mortgage_custrep_all_vw')
#table_list.append('mortgage_warehouse')
table_list.append('mortgage_warehouse_export')  
table_list.append('review_view')
table_list.append('stsd_warehouse_export')
table_list.append('test_results_review_level')
table_list.append('test_results_disclosure_level')

table_list.append('xref_uca_warehouse_export')




dbPOSTGRES.print_tables(table_list)
#migu.print_create_table(db,folder="/home/cfpb/nguyenhu/compliancesql",targetschema="compliance")
#migu.print_create_table(dbPOSTGRES,folder="/home/cfpb/nguyenhu/compliancepreysql",targetschema="stg")
#migu.print_create_table(db,targetschema="stg")
#dbPOSTGRES.print_drop_tables()

#migu.reset_migration(dbPOSTGRES) 
#migu.print_sqitch_files("../migrate/enforce/deploy/tables","sql","tables")

