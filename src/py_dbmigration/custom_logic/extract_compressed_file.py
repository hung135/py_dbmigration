
import os, logging as lg

logging=lg.getLogger() 
import sys
from py_dbutils.rdbms.postgres import DB
from py_dbmigration.data_file_mgnt.data_files import DataFile 
import py_dbmigration.zip_utils as zip_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI



'''
    File name: extract_compressed_file.py
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 3.6
    Descripton:
    #  This logic will extract a compressed zip file and inventory the files extracted and link it to the parent_file_id
        - logic: 
            name: 'custom_logic.extract_compressed_file'
            skip_extract: False # if unzipped directory found we won't reexctract defaults to false if not specified
    #  
'''


def custom_logic(db: DB, foi: FOI, df: DataFile,logic_status: LogicState):

    file_id = df.file_id
    
    abs_file_path = logic_status.file_state.file_path
    abs_writable_path = os.path.join(df.working_path, df.curr_src_working_file)


# def extract_file(self, db, abs_file_path, abs_writable_path, skip_ifexists=False):

    try:
        logic_dict=search_foi_yaml(foi)
        skip_extract=False
        if logic_dict:
            skip_extract=logic_dict.get('skip_extract',False)
        md5 = logic_status.row.crc
        modified_write_path = os.path.join(abs_writable_path, str(md5)[:8])

        files = zip_utils.unzipper.extract_file(abs_file_path, modified_write_path,
                                                False, df.work_file_type, skip_ifexists=skip_extract)

        total_files = len(files)

        logic_status.row.total_files = total_files
    # We walk the tmp dir and add those data files to list of to do
        new_src_dir = modified_write_path
        logging.debug(
            "WALKING EXTRACTED FILES:\nsrc_dir:{0} \nworking_dir:{1}: --{2}".format(new_src_dir, df.working_path, modified_write_path))

        file_table_map = [FOI(file_type='DATA', file_regex='.*', file_path=modified_write_path,  
                                                                    parent_file_id=file_id, project_name=foi.project_name)]

        # instantiate a new Datafile object that craw this new directory of extracted files
        DataFile(
            new_src_dir, db, file_table_map, parent_file_id=file_id)
    except Exception as e:
        logging.exception(f"Failed Extracting: {e}")
        logic_status.failed(e)
    
    return logic_status


#function to iterate over yaml definition looking for logic or plugin optional parameters
def search_foi_yaml(foi: FOI):

    logic_dict=None
    file_file="custom_logic.extract_compressed_file"
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
