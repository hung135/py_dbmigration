
 

import sys
from py_dbutils import parents as db_utils
from .. import data_file_mgnt
from .. import migrate_utils

from py_dbmigration.data_file_mgnt.state import *
import os, logging as log
logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)


'''
    File name: generate_checksum.py
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # get the file size and put it into meta_source_files
'''
 
def custom_logic(db, foi, df, logic_status):

    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):

    abs_file_path = logic_status.file_state.file_path

    file_size = os.path.getsize(abs_file_path)
    file_size_mb = round(file_size * 1.0 / 1024 / 1024, 2)
    logging.info("\t\t\tFile Size: {} MB ".format(file_size_mb))
    logic_status.row.file_size = file_size_mb
    
    return logic_status
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
