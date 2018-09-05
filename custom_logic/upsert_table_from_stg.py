
import logging
import os
import sys
import db_utils
import data_file_mgnt
import migrate_utils
logging.basicConfig(level='DEBUG')

''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # after loading data into staging table
    # this code will upsert the data into the table residing in the data_schema
    # target table must have a primary key
    
'''

# sql used to update logging....faster than any framework wrapper
#update_sql = """UPDATE logging.meta_source_files set  crc='{}'  where id = {}"""


def custom_logic(db, foi, df):
    table_name = foi.table_name
    data_schema = db.dbschema
    stage_schema = 'stg'
    primary_key = db.get_primary_keys(data_schema+"."+table_name)
    upsert_sql = migrate_utils.static_func.generate_postgres_upsert(
        db, table_name, stage_schema, data_schema, df.meta_source_file_id)

    db.execute_permit_execption("update stg.{} set file_id={}".format(table_name, df.meta_source_file_id))
    db.execute_permit_execption(upsert_sql)

    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    continue_processing = True

    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.dbconn.Connection)

    return custom_logic(db, foi, df)
