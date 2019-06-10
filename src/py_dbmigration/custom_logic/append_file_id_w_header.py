
import logging
import os
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
import subprocess
logging.basicConfig(level='DEBUG')


'''
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # givent a file will append a file_id to a working_directory
'''

# sql used to update logging....faster than any framework wrapper


def append_file_id(orgfile, newfile,   delimiter,  file_id, has_header=False):

    cmd_sed = "sed 's/^/{}{}/' {} >>{}".format(file_id, delimiter, orgfile, newfile)

    logging.info("\t\t----SED--- to Add file_id", cmd_sed)
    subprocess.call([cmd_string_double_quote], shell=True)


def custom_logic(db, foi, df,logic_status):

    continue_processing = False
    file_id = df.meta_source_file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    append_file_id(abs_file_path, abs_file_path+"_modified", foi.delimiter, df.meta_source_file_id)

    df.curr_src_working_file = df.curr_src_working_file+"_modified"

    if rows_updated == 0:
        raise ValueError('Unexpected thing happend now rows updated')

    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
