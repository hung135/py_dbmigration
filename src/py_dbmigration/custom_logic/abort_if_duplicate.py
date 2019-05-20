
import logging
import os
import sys
import db_utils
import data_file_mgnt
import migrate_utils
import db_table
logging.basicConfig(level='DEBUG')

''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    #  if this is a duplicate file abort the process for this file
'''

# sql used to update logging....faster than any framework wrapper


def custom_logic(db, foi, df):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):

    continue_processing = True
    already_processed = db.has_record(
        """select 1 from logging.meta_source_files a,logging.meta_source_files b
                    where a.file_process_state='Processed'
                    and b.crc=a.crc and a.id!=b.id
                    and b.id={}
                    and a.project_name=b.project_name
                    """.format(df.meta_source_file_id))

    if already_processed:
        # raise execption to continue with the next file
        # raise valuerror to abort process
        logging.error("\t\tDuplicate File Found")
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        row = t.get_record(db_table.db_table_def.MetaSourceFiles.id == df.meta_source_file_id)
        row.duplicate_file = True
        t.session.commit()
        t.session.close()
        df.load_status_msg = 'Duplicate File Found'
        continue_processing = False
    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    # variables expected to be populated

    error_msg = None
    additional_msg = None

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.dbconn.Connection)
    return custom_logic(db, foi, df)
