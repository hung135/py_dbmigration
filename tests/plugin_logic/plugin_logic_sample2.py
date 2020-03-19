

import sys
from py_dbutils.rdbms.postgres import DB 
from py_dbmigration.data_file_mgnt.state import FOI, LogicState
from py_dbmigration.data_file_mgnt.data_files import DataFile
import os
import logging



'''
  
    Author: Hung Nguyen
    Python Version: 3.6
    Descripton:
    # sample file to show logic will get executed
'''


def custom_logic(db: DB, foi: FOI, df: DataFile,logic_status: LogicState):
    print("------------------------------This Logic does nothing------------------------------")

    print("------------------------------",__file__,"------------------------------")

    return logic_status


def process(db, foi, df, logic_status):
    # assert isinstance(foi, FOI)
    # assert isinstance(db, dbconnection)
    # assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
