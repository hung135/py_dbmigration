
import logging
import os

import py_dbutils.parents as db_utils
from py_dbmigration import data_file_mgnt
from py_dbmigration import migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
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


def custom_logic(db, foi, df, logic_status):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):

    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    if logic_status.row.crc is not None:
        logging.info("\t\tChecksum already Exists, Skipping:")
    else:
        logging.info("\t\tCheck Not Exists, generating MD5:")
        crc = migrate_utils.static_func.md5_file_36(abs_file_path)
        logging.info("\t\t\tMD5: {}".format(crc))
        logic_status.row.crc = crc
        logic_status.table.session.commit()

    return logic_status
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
 
     
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
