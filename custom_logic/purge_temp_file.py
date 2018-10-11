
import logging
import os
import sys
import db_utils
import data_file_mgnt
import migrate_utils
import shutil
import time
logging.basicConfig(level='DEBUG')

''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # temporary
'''

check_finish_sql ="select p.id,p.file_name,c.file_path from logging.meta_source_files c, logging.finished_imports p where c.parent_file_id=p.id and c.id={}"
 


def custom_logic(db, foi, df):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    continue_processing = True

    parent_file_id=db.query(check_finish_sql.format(df.meta_source_file_id))
 
    #print(parent_file_id)
    for id,file_name,file_path in parent_file_id:
            delete_path=file_path.split(file_name)
            delete_path=os.path.join(delete_path[0],file_name)

            #hardcoded to project accidental deletion of source data directory
            if('/home/dtwork/dw/file_transfers'  in delete_path):
                #print("Deleting....temp files",delete_path)
                logging.debug("All files imported removing Tempfiles: \n\t{}".format(delete_path))
                shutil.rmtree(delete_path)

 
    return continue_processing

# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.dbconn.Connection)

    return custom_logic(db, foi, df)
