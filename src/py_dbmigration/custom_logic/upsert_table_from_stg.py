
 
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
 
import os, logging as lg

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


def custom_logic(db, foi, df,logic_status):
    continue_processing = True
    table_name = foi.table_name
    data_schema = db.dbschema
    stage_schema = 'stg'
    primary_key = db.get_primary_keys(data_schema + "." + table_name)
    update_sql = None

    if len(primary_key) == 0:
        no_upsert_sql = """insert into {target_schema}.{table_name} ({column_list}) select {column_list} from {src_schema}.{table_name}"""
        col_list = db.get_columns(table_name, data_schema)
        colums_str = "\"" + "\", \"".join(col_list) + "\""
        update_sql = no_upsert_sql.format(column_list=colums_str, target_schema=data_schema,
                                          src_schema=stage_schema, table_name=table_name)
        logging.info("\t\tNo Primary Key found, Loading directly:")
    else:
        upsert_sql = migrate_utils.static_func.generate_postgres_upsert(
            db, table_name, stage_schema, data_schema, df.file_id)

        update_sql = upsert_sql
        logging.info("\t\tPrimary Found Loading via Upsert: {}".format(primary_key))

    logging.info("\t\tSetting file_id to: {}".format(df.file_id))
    cnt = db.execute("update stg.{} set file_id={}".format(table_name, df.file_id),catch_exception=False)
    logging.info("\t\tLoading...could take a while: {} Records:".format(cnt))
    logging.debug(update_sql)
    x = db.execute(update_sql,catch_exception=False)
    logging.info("\t\tUpsert compleated: {}".format(x))
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):

    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
