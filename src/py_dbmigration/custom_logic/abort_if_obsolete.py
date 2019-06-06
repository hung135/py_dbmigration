
import logging
import os
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.structs import Status, import_status
 

logging.basicConfig(level='DEBUG')

''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    #  if this is file is outdated and should be ignored if a new field exists
    # this logic will abort and tag the file as being obsolete
'''

# sql used to update logging....faster than any framework wrapper


def custom_logic(db, foi, df,logic_status):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    

    file_id=df.meta_source_file_id
     
    file_exists = db.has_record(
        """select 1
                from logging.meta_source_files  cur,logging.meta_source_files  newer
                where 
                cur.file_name_data<=newer.file_name_data
                and cur.id={file_id}
                and (concat(newer.file_path,newer.file_name) ~  '{file_regex}')
                and cur.project_name=newer.project_name
                and cur.file_name<=newer.file_name
                and cur.id!=newer.id
                and cur.file_type=newer.file_type
                limit 1
                    """.format(file_id=df.meta_source_file_id, file_regex=foi.regex))

    if file_exists:
        # raise execption to continue with the next file
        # raise valuerror to abort process
        logging.error("\t\tObsolete Data File: Newer File Found")
    
        logic_status.continue_processing = False
        logic_status.import_status.import.OBSOLETE
        logic_status.status='Obsolete Data File: Newer File Found'
        logic_status.additional_msg='Obsolete Data File: Newer File Found'
    return logic_status


def process(db, foi, df):
    # variables expected to be populated

    error_msg = None
    additional_msg = None

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.DB)
    logic_status=Status(file=__file__)
    return custom_logic(db, foi, df,logic_status)
