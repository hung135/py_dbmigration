
import logging
import os
import sys
import db_utils
import data_file_mgnt
import migrate_utils
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


def custom_logic(db, foi, df):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    continue_processing = True
    table_name = foi.table_name
    target_schema = foi.schema_name
    file_id = df.meta_source_file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    file_size = os.path.getsize(abs_file_path)

    file_size_mb = round(file_size * 1.0 / 1024 / 1024, 2)
    logging.info("\t\tFile Size: {} MB ".format(file_size_mb))
    rows_updated = db.execute(update_sql.format(file_size, file_id))

    if rows_updated == 0:
        raise ValueError('Unexpected thing happend now rows updated')
    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.DB)

    return custom_logic(db, foi, df)
