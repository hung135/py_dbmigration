
 
import sys
import py_dbutils.rdbms.postgres as db_utils 
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
import shutil
import datetime
import os, logging




''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # temporary
'''
vw_finished_import="""SELECT DISTINCT p.id,
    p.file_name_data,
    p.file_name,
    c.last_error_msg
   FROM logging.meta_source_files p
     LEFT JOIN logging.meta_source_files c ON c.parent_file_id = p.id AND c.file_process_state::text <> 'PROCESSED'::text
  WHERE p.parent_file_id = 0 AND p.file_process_state::text = 'PROCESSED'::text AND c.id IS NULL"""

check_finish_sql ="select p.id,p.file_name,c.file_path from logging.meta_source_files c, ({}) p where c.parent_file_id=p.id and c.id={}"
 


def custom_logic(db, foi, df):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    continue_processing = True
    try:
        
        if not 's3://' in df.source_file_path:

            parent_file_id,x=db.query(check_finish_sql.format(vw_finished_import,df.file_id))
        #print(parent_file_id)
            for id,file_name,file_path in parent_file_id:
                    delete_path=file_path.split(file_name)
                    delete_path=os.path.join(delete_path[0],file_name)
        
                    #hardcoded to project accidental deletion of source data directory
                    if('/home/dtwork/dw/file_transfers'  in delete_path):
                        #print("Deleting....temp files",delete_path)
                        logging.debug("All files imported removing Tempfiles: \n\t{}".format(delete_path))
                        shutil.rmtree(delete_path)
        else:
            logging.info("Will not delete from s3")

    except Exception as e:
        logging.exception("Error in purge_temp_file : {}".format(e))
        continue_processing=False
    return continue_processing

# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
 
    
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
 
    return custom_logic(db, foi, df)
