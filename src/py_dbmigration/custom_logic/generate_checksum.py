
import logging
import os
 
import py_dbutils.parents as db_utils
from py_dbmigration import data_file_mgnt
from py_dbmigration import migrate_utils
logging.basicConfig(level='DEBUG')

''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    #  this function will add a crc column to the table if not existing and will generate a checksum based on columns passed in
    #  else it will pull list of columns from db and generate checksum for those columns and set the crc
'''

# sql used to update logging....faster than any framework wrapper
update_sql = """UPDATE logging.meta_source_files set  crc='{}'  where id = {}"""


def custom_logic(db, foi, df):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    continue_processing = False
    table_name = foi.table_name
    target_schema = foi.schema_name
    file_id = df.meta_source_file_id
 
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)
    sql = "select 1 from logging.meta_source_files where id = {} and crc is not null limit 1".format(df.meta_source_file_id)
    
 
    if db.has_record(sql):
        logging.info("\t\tChecksum already Exists, skipping:")
    else:
        logging.info("\t\tCheck Not Exists, generating:")
        crc = migrate_utils.static_func.md5_file_36(abs_file_path)
  
        rows_updated = db.execute(update_sql.format(crc, file_id))
       
        if rows_updated == 0:
            raise ValueError('Unexpected thing happend no rows updated')
  
    continue_processing=True
    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None
     
    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.DB)
     
    return custom_logic(db, foi, df)
