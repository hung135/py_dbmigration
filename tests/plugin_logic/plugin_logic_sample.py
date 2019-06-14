

import sys
from py_dbutils.parents import DB as dbconnection
from py_dbmigration.data_file_mgnt.state import FOI, LogicState
import os
import logging

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')

'''
  
    Author: Hung Nguyen
    Python Version: 3.6
    Descripton:
    # sample file to show logic will get executed
'''


def custom_logic(db, foi, df, logic_status):
    print("------------------------------This Logic does nothing------------------------------")

    return logic_status


def process(db, foi, df, logic_status):
    assert isinstance(foi, FOI)
    assert isinstance(db, dbconnection)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
