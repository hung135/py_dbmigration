
import os, logging

import sys
import py_dbutils.parents as db_utils
 
from py_dbmigration.data_file_mgnt.state import *
import py_dbmigration.migrate_utils as migrate_utils
import py_dbmigration.db_table as db_table
#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')


''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    #  if this is a duplicate file abort the process for this file
'''

# sql used to update logging....faster than any framework wrapper


def custom_logic(db, foi, df, logic_status):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):

    already_processed = db.has_record(
        """select 1 from logging.meta_source_files a,logging.meta_source_files b
                    where a.file_process_state='PROCESSED'
                    and b.crc=a.crc and a.id!=b.id
                    and b.id={}
                    and a.project_name=b.project_name
                    """.format(df.meta_source_file_id))

    if already_processed:
        # raise execption to continue with the next file
        # raise valuerror to abort process
        logging.debug("\t\tDuplicate File Found")

        # let file state machine determin if we can continue
        logic_status.continue_to_next_logic(
            logic_status.file_state.duplicate())
    else:
        logging.debug("\t\tDuplicate NOT File Found")
    return logic_status
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df, logic_status):
    # variables expected to be populated

    assert isinstance(foi, FOI)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
