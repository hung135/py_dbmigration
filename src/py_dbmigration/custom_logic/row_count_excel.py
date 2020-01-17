
 
 
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
 
import os, logging




'''
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # get the call the linux wc command and cound the rows in the file
'''

# sql used to update logging....faster than any framework wrapper
update_sql = """UPDATE logging.meta_source_files set total_rows={}  where id = {}"""


def custom_logic(db, foi, df,logic_status):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    continue_processing = True
    file_id = df.file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    data_value, dummy_column_count = migrate_utils.static_func.count_excel(abs_file_path)
    logging.info("Row Count for Excel File: {} ".format(data_value))
    rows_updated = db.execute(update_sql.format(data_value, file_id))

    if rows_updated == 0:
        raise ValueError('Unexpected thing happend now rows updated')
    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
