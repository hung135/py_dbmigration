
import logging
import os
import sys
from py_dbutils import parents as db_utils
from .. import data_file_mgnt
from .. import migrate_utils

from py_dbmigration.data_file_mgnt.state import *
logging.basicConfig(level='DEBUG')

'''
    File name: generate_checksum.py
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # get the file size and put it into meta_source_files
'''

# sql used to update logging....faster than any framework wrapper
update_sql = """UPDATE logging.meta_source_files set file_size={}  where id = {}"""


def custom_logic(db, foi, df,logic_status):
 
    
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
 
     
    file_id = df.meta_source_file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    file_size = os.path.getsize(abs_file_path)

    file_size_mb = round(file_size * 1.0 / 1024 / 1024, 2)
    logging.info("\t\tFile Size: {} MB ".format(file_size_mb))
    try:
        db.execute(update_sql.format(file_size, file_id))
    except Exception as e:
        logic_status.import_status=import_status.FAILED
        logic_status.error_msg=str(e)
        logic_status.continue_processing=False
         
    return logic_status
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
