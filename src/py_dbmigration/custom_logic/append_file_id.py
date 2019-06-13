
 
 
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
import subprocess
import os, logging as log
logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)

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

    cmd_sed = "sed 's/^/{}{}/' {} >{}".format(file_id, delimiter, orgfile, newfile)
    # print(cmd_sed)
    logging.info("\t\t----SED--- to Add file_id:\n\t\t{}".format(cmd_sed))
    subprocess.call([cmd_sed], shell=True)


def custom_logic(db, foi, df,logic_status):

    continue_processing = False
    file_id = df.meta_source_file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    append_file_id(abs_file_path, abs_file_path+"_modified", foi.file_delimiter, df.meta_source_file_id)

    df.curr_src_working_file = df.curr_src_working_file+"_modified"

    continue_processing = True

    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
    error_msg = None
    additional_msg = None

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
