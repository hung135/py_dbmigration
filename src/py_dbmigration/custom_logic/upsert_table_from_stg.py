
import os
import logging as lg
import sys
from py_dbutils.rdbms.postgres import DB
from py_dbmigration.data_file_mgnt.data_files import DataFile  
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
import py_dbmigration.migrate_utils as migrate_utils 
 

logging=lg.getLogger() 


'''
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # after loading data into staging table
    # this code will upsert the data into the table residing in the data_schema
    # target table should  have a primary key
    # if no primary key is found it will load the data in without any upsert logic

'''

# sql used to update logging....faster than any framework wrapper
# update_sql = """UPDATE logging.meta_source_files set  crc='{}'  where id = {}"""


def custom_logic(db: DB, foi: FOI, df: DataFile,logic_status: LogicState):
    continue_processing = True
    trg_table_name = foi.table_name
    src_table_name = foi.table_name
    trg_schema = db.dbschema
    src_schema = 'stg'
    primary_key = db.get_primary_keys(trg_schema + "." + trg_table_name)
    update_sql = None
    if not isinstance(foi.CURRENT_LOGIC_CONFIG,str):
        trg_schema=foi.CURRENT_LOGIC_CONFIG.get('trg_schema',trg_schema)
        src_schema=foi.CURRENT_LOGIC_CONFIG.get('src_schema',src_schema)
        trg_table_name=foi.CURRENT_LOGIC_CONFIG.get('trg_table_name',trg_table_name)
        src_table_name=foi.CURRENT_LOGIC_CONFIG.get('src_table_name',src_table_name)


    if len(primary_key) == 0:
        no_upsert_sql = """insert into {target_schema}.{trg_table_name} ({column_list}) 
        select {column_list} from {src_schema}.{src_table_name}"""
        col_list = db.get_columns(trg_table_name, trg_schema)
        colums_str = "\"" + "\", \"".join(col_list) + "\""
        update_sql = no_upsert_sql.format(column_list=colums_str, target_schema=trg_schema,
                                          src_schema=src_schema, trg_table_name=trg_table_name,
                                          src_table_name=src_table_name)
        logging.info("\t\tNo Primary Key found, Loading directly:")
    else:
        upsert_sql = migrate_utils.static_func.generate_postgres_upsert(
            db, src_table_name,trg_table_name, src_schema, trg_schema)

        update_sql = upsert_sql
        logging.info("\t\tPrimary Found Loading via Upsert: {}".format(primary_key))
    logging.info("\t\tLoading...could take a while:")
    logging.debug(update_sql)
    x = db.execute(update_sql,catch_exception=False)
    logging.debug("\t\tUpsert completed: {}".format(x))
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):

    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df, logic_status):
    
    assert isinstance(foi,FOI)
    assert isinstance(db, DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
