
import logging
 
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
from shutil import copyfile
import datetime
from datetime import date
import os, logging

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')



'''
    Author: Sally jew
    Date created: 02/26/2020
    Date last modified: 02/26/2020
    Python Version: 3.x
    Descripton:
    # copy file to outbound directory for processing
'''

# NB: Do not use this on large files as it would be horribily slow


def copy_file(orgfile, newfile):

    print(orgfile)
    print(newfile)
    copyfile(orgfile, newfile)
 

def custom_logic(db, foi, df,logic_status):

    print('foi.outbound_file_path')
    print(foi.outbound_file_path)
    print(df.file_id)

    # get file_name_data (date) from the file name
    # get from file name or get from parent file name
    query = """SELECT replace(file_name_data, '-', '')
      FROM logging.meta_source_files
      WHERE id = {}"""
    print(query)
    
    resultset, dummy = db.query(query.format(df.file_id))

    if len(resultset) > 0:
      for r in resultset:
        file_name_data = r[0]
        
        if file_name_data=='':
          today=date.today()
          file_name_data=today.strftime("%Y%m%d")

        print(file_name_data) 
    else:
      print('file_name_data is empty')
      today=date.today()
      file_name_data=today.strftime("%Y%m%d")

    try:
      source_file = os.path.join(df.source_file_path, df.curr_src_working_file)
      out_file_path = foi.outbound_file_path + '/' + file_name_data
      print (out_file_path)
      # Add code to check whether dir exists, if not create it
      if not os.path.exists(out_file_path):
        os.mkdir(out_file_path)

      outbound_file = os.path.join(out_file_path, df.curr_src_working_file)
      copy_file(source_file, outbound_file)

    except Exception as e:
            logging.exception(e)
            logic_status.failed(e)

    return logic_status


# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
 

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
