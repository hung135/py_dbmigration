# import csv, pandas,        sqlalchemy, os
import os
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import DataFileState,FileStateEnum,LogicState,LogicStateEnum
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
import pprint

import logging

logging.basicConfig(level='DEBUG')
file_path = os.environ['RAWFILEPATH']
writable_path = os.environ['WORKINGPATH']
"""
    Creating a Database connection object
"""
db = db_utils.DB(schema='stg')
db.truncate_table("logging.error_log")
db.truncate_table("logging.load_status")
# db.truncate_table("compliance.meta_source_files")


# def walk_dir(file_path, schema,pattern):
# regex = re.compile(r"^\d\d\d\dQ\d\.zip$",re.IGNORECASE)
# regex = re.compile(r"^\d\d[1][2:3]q\d\.zip$", re.IGNORECASE)

FILE_REGEX = r"^ComplianceAnalysis.*tar.gz$"

FILE_REGEX = r"^.*.csv$"
file_path = '/home/dtwork/dw/file_transfers/compliance/allCSVs.zip/'
FILE_ID_REGEX = r"\d\d\d\d\d\d\d\d"
FILE_ID_REGEX = None
df = dfm.DataFile(file_path, writable_path, FILE_REGEX, db, FILE_ID_REGEX,
                  file_type=dfm.DataFile.FILE_TYPE_CSV)
"""
    Creating object to define the data file wee need to process and which table they need to be imported into
    DEFINE ALL FILE PATTERN AND MAPP TO OBJECT BELOW
    COMNPLIANCE HAS 13 FILES INSIDE OF TAR.GZ
"""
SCHEMA = "stg"
DELIMITER = ','
COLUMN_LIST = ''

datafiles = [dfm.FileOfInterest('account', r'^Account\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True)]
"""
datafiles.append(dfm.FileOfInterest('AccountInterest', r'^AccountInterest\d.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('AccountReview', r'^AccountReview\d.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

datafiles.append(dfm.FileOfInterest('AccountStmt', r'^AccountStmt\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('Client_Address', r'^Client_Address\d.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('Client_User', r'^Client_User\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('Client', r'^Client\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('CR_DISCLOSURE_VIEW', r'^CR_DISCLOSURE_VIEW\d.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('Fees', r'^Fees\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('MortgageCDE', r'^MortgageCDE\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('Review', r'^Review\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('TEST_RESULTS_DISCLOSURE_LEVEL',
                                   r'^TEST_RESULTS_DISCLOSURE_LEVEL\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('XREF_User_Client_Access', r'^XREF_User_Client_Access\d.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('STSDCDE', r'^STSDCDE\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

datafiles.append(dfm.FileOfInterest('Loan', r'^Loan\d.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

 
truncate table stg.account;
truncate table stg.AccountInterest;
truncate table stg.accountreview;
truncate table stg.client_user_warehouse_export; 
truncate table stg.cr_disclosure_view;
truncate table stg.fees_view;
truncate table stg.loan_view;
truncate table stg.mortgage_warehouse_export; 
truncate table stg.review_view;
truncate table stg.test_results_review_level; 
truncate table stg.xref_uca_warehouse_export;


"""

datafiles.append(dfm.FileOfInterest('review_view', r'^REVIEW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

datafiles.append(dfm.FileOfInterest('account', r'^ACCOUNT_VW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('accountreview', r'^ACCOUNT_REVIEW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('accountinterest', r'^ACCOUNT_INT_VW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('accountstmt', r'^ACCOUNT_STMT.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('cr_disclosure_view', r'^CR_DISCLOSURE_VW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

datafiles.append(dfm.FileOfInterest('loan_view', r'^LOAN_VW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

datafiles.append(dfm.FileOfInterest('xref_uca_warehouse_export', r'^XREF_UCA.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('fees_view', r'^FEES_VW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('client_user_warehouse_export', r'^CLIENT_USER.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('mortgage_warehouse_export', r'^MORTGAGE_WAREHOUSE.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('test_results_review_level', r'^TEST_RESULT.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

datafiles.append(dfm.FileOfInterest('stsd_warehouse_export', r'^STSD_WAREHOUSE_EXPORT.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('test_results_disclosure_level', r'^TEST_RSLTS_DISCLOSURE.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('mortgage_warehouse_export', r'^MRTG_WRHSE_EXP_VW.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('mortgage_custrep_all_vw', r'^MORTGAGE_CUSTREP.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('client_add_warehouse_export', r'^CLIENT_ADDRESS.*.csv',
                                   DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))
datafiles.append(dfm.FileOfInterest('client', r'^CLIENT-.*.csv', DELIMITER, COLUMN_LIST, SCHEMA, has_header=True))

# df.reset_meta_table(db, 'ALL')
# df.do_work(db,datafiles,COMPRESSED_FILE_TYPE)


FILE_REGEX = r"XFull.*\d\d\d\d\d.zip$"

FILE_ID_REGEX = r"\d\d\d\d\d\d\d\d"
df2 = dfm.DataFile(file_path, writable_path, FILE_REGEX, db, FILE_ID_REGEX,dfm.DataFile.FILE_TYPE_ZIP)
"""
# implmented folder regex to machine file to database table
datafiles.append(dfm.FileOfInterest('full_multi', r'.*fees.csv', DELIMITER,
                                   COLUMN_LIST, SCHEMA, has_header=True, folder_regex=r'.*multi.*'))
datafiles.append(dfm.FileOfInterest('full_multi', r'.*uploaded.csv', DELIMITER,
                                   COLUMN_LIST, SCHEMA, has_header=True, folder_regex=r'.*mult.*'))
datafiles.append(dfm.FileOfInterest('full_single', r'.*fees.csv', DELIMITER,
                                   COLUMN_LIST, SCHEMA, has_header=True, folder_regex=r'.*singl.*'))
datafiles.append(dfm.FileOfInterest('full_single', r'.*uploaded.csv', DELIMITER,
                                   COLUMN_LIST, SCHEMA, has_header=True, folder_regex=r'.*singl.*'))

"""
# df.reset_meta_table(db, 'ALL')

# df2.reset_meta_table(db, 'FAILED')
# only need one of the instance to do work

# df.do_work(db, datafiles, cleanup=False, limit_rows=None,import_type='Pandas')
assert isinstance(datafiles, list)
assert isinstance(datafiles[0], dfm.FileOfInterest)
df.do_work(db, datafiles, cleanup=False, limit_rows=None, import_type='CopyCommand')

# pprint.pprint(db.get_tables_row_count('stg'))
# df.reset_meta_table(db, 'ALL')
# df2.reset_meta_table(db, 'ALL')


"""
select * from (select 'account - imported',count(*) as cnt from stg.account
union all  select 'test_results_disclosure_level - imported',count(*) from stg.test_results_disclosure_level
union all  select 'accountreview - imported',count(*) from stg.accountreview
union all  select 'cr_disclosure_view - imported',count(*) from stg.cr_disclosure_view
union all  select 'fees_view - imported',count(*) from stg.fees_view
union all  select 'test_results_review_level - imported',count(*) from stg.test_results_review_level
union all  select 'client - imported',count(*) from stg.client
union all  select 'stsd_warehouse_export - imported',count(*) from stg.stsd_warehouse_export
union all  select 'accountstmt - imported',count(*) from stg.accountstmt
union all  select 'loan_view - imported',count(*) from stg.loan_view
union all  select 'mortgage_warehouse_export - imported',count(*) from stg.mortgage_warehouse_export
union all  select 'accountinterest - imported',count(*) from stg.accountinterest
union all  select 'client_add_warehouse_export - imported',count(*) from stg.client_add_warehouse_export
union all  select 'client_user_warehouse_export - imported',count(*) from stg.client_user_warehouse_export
union all  select 'review_view - imported',count(*) from stg.review_view
union all  select 'mortgage_custrep_all_vw - imported',count(*) from stg.mortgage_custrep_all_vw
union all  select 'xref_uca_warehouse_export - imported',count(*) from stg.xref_uca_warehouse_export
union
 select database_table,sum(total_rows) as cnt from stg.meta_source_files
group by database_table ) order by 1

"""
