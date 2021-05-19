
import os, logging as lg

logging=lg.getLogger() 
import sys
from py_dbutils.rdbms.postgres import DB
from py_dbmigration.data_file_mgnt.data_files import DataFile,FilesOfInterest 
import py_dbmigration.zip_utils as zip_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
import re


'''
    File name: update_child_file.py
    Author: Hung Nguyen
    Date created: 5/17/20201
    Date last modified: 5/17/2021
    Python Version: 3.6
    Descripton:
    #  This logic will update children files to the current file with data extracted from the full path and put into the file_name_data 
    #  of both parent and child files
    #    extract_regex: '\d\d\d\d\d\d'
    #    format_extracted_date: 'YYYYMM' (not implmented)
'''


def custom_logic(db: DB, foi: FOI, df: DataFile,logic_status: LogicState):

    file_id = df.file_id 
    abs_file_path = logic_status.file_state.file_path
    #abs_writable_path = os.path.join(df.working_path, df.curr_src_working_file)


# def extract_file(self, db, abs_file_path, abs_writable_path, skip_ifexists=False):

    try:
    
         
        logic_dict=search_foi_yaml(foi)

 
        extract_regex=logic_dict['extract_regex']
        option=logic_dict.get('option','full_path')
        m=None
        if option=='full_path':
             m = re.search(extract_regex, abs_file_path)
        elif option=='file_name':
            m = re.search(extract_regex, df.curr_src_working_file)
        else:
            raise Exception("Uknown Option Paramter",option)
 
        if m:
            extracted_value=m.group()
        else:
            raise Exception('No Data Extract Found for RGEX:', extract_regex)
        parent_sql = f"update logging.meta_source_files set file_name_data='{extracted_value}' where id={file_id}"
        child_sql = f"update logging.meta_source_files set file_name_data='{extracted_value}' where parent_file_id={file_id}"
        db.execute(parent_sql)
        db.execute(child_sql)
 
    except Exception as e:
        logging.exception(f"Failed Updating DB: {e}")
        logic_status.failed(e)
    
    return logic_status



#function to iterate over yaml definition looking for logic or plugin optional parameters
def search_foi_yaml(foi: FOI):

    logic_dict=None
    file_file="custom_logic.update_child_file"
    logic_file=os.path.basename(__file__)
    
    for x in foi.process_logic: 
        if isinstance(x.get('logic'), dict):
            if str(x.get('logic').get('name'))==str(file_file):
                logic_dict=x.get('logic') 
        if isinstance(x.get('plugin'), dict):
            if str(logic_file) == os.path.basename(str(x.get('plugin').get('name'))):
                logic_dict=x.get('plugin') 
    return logic_dict
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any
def process(db, foi, df, logic_status):

    assert isinstance(foi,FOI)
    assert isinstance(db, DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
