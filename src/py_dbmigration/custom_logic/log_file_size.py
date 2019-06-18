

import sys
from py_dbutils.rdbms.postgres import DB 
from py_dbmigration.data_file_mgnt.state import FOI, LogicState
from py_dbmigration.data_file_mgnt.data_files import DataFile

import os
import logging

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')

'''
  
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 3.6
    Descripton:
    # get the file size and put it into meta_source_files
'''


def custom_logic(db: DB, foi: FOI, df: DataFile,logic_status: LogicState):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    abs_file_path = logic_status.file_state.file_path
    file_size = os.path.getsize(abs_file_path)
    file_size_mb = round(file_size * 1.0 / 1024 / 1024, 2)
    logging.debug("\t\t\tFile Size: {} MB ".format(file_size_mb))
    logic_status.row.file_size = file_size_mb

    return logic_status


def process(db, foi, df, logic_status):
    assert isinstance(foi, FOI)
    assert isinstance(db, DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
