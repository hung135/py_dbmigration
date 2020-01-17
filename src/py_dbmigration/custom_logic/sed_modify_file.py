
import logging
 
import sys
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
import subprocess
import datetime
import os, logging





'''
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    # givent a file will append a file_id to a working_directory
'''

# NB: Do not use this on large files as it would be horribily slow


def modify_file(orgfile, newfile,   delimiter,  file_id, has_header=False):

    sed_list=[]
     
    sed_list.append("^In Foreclosure, Presale")
    sed_list.append("^In Foreclosure, Postsale")
    sed_list.append('^CRA, House America')
    sed_list.append('^Interest Only, other than monthly')
    sed_list.append('^Interest Only, monthly')
    sed_list.append('^Second Lien Home Equity Line of Credit, Grade "B" or "C"')
    sed_list.append('^First Lien Home Equity Line of Credit, Grade "B" or "C"')
    sed_list.append('^Second Mortgage (Home Equity Loan), Grade "B" or "C"')
    sed_list.append('^First Mortgage, Grade "B" or "C"')

    print(orgfile)
    print(newfile)
    cmd_sed = "sed 's/*/*/' {} >{}".format(  orgfile, newfile)
    subprocess.call([cmd_sed], shell=True)
    print(cmd_sed)
    for x in sed_list:
        cmd_sed = "sed -i 's/{}/\"{}\"/' {} ".format(x,x.replace('"','""'),newfile)
        subprocess.call([cmd_sed], shell=True)
        #print(cmd_sed)
    #time.sleep(10)
 

def custom_logic(db, foi, df,logic_status):

    continue_processing = False
    file_id = df.file_id
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    modify_file(abs_file_path, abs_file_path+"_modified", foi.file_delimiter, df.file_id)

    df.curr_src_working_file = df.curr_src_working_file+"_modified"

    continue_processing = True

    return continue_processing
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df):
 

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
