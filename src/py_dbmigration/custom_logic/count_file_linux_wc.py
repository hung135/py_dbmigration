
import logging
import os
 
from py_dbutils import parents as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import DataFileState,FileStateEnum,LogicState,LogicStateEnum
logging.basicConfig(level='DEBUG')

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
    # you have to return this boolean to let the framework now to continue w/ the rest of the processing logic
    
    file_id = df.meta_source_file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)
     
    try:
        data_value = migrate_utils.static_func.count_file_lines_wc_36(abs_file_path)
        db.execute(update_sql.format(data_value, file_id))
        logic_status.status='COMPLETE'
        
    except Exception as e:
        logic_status.continue_processing=False
        logic_status.import_status=import_status.FAILED
        logic_status.error_msg=e


    return logic_status
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
    # variables expected to be populated
 
    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
