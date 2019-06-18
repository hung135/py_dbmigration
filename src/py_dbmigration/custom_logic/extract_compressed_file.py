
import os, logging

 
import sys
import py_dbutils.parents as db_utils
import py_dbmigration.data_file_mgnt.data_files as data_files 
import py_dbmigration.db_table as db_table
import py_dbmigration.zip_utils as zip_utils
from py_dbmigration.data_file_mgnt.state import LogicState,FOI
#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')


'''
    File name: generate_checksum.py
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 3.6
    Descripton:
    #  This logic will extract a compressed zip file and inventory the files extracted and link it to the parent_file_id
    #  
    #  
'''


def custom_logic(db: db_utils.DB, foi: FOI, df: DataFile,logic_status: LogicState):

    file_id = df.file_id
    skip_ifexists = (not foi.unzip_again)
    abs_file_path = logic_status.file_state.file_path
    abs_writable_path = os.path.join(df.working_path, df.curr_src_working_file)


# def extract_file(self, db, abs_file_path, abs_writable_path, skip_ifexists=False):

    try:

        md5 = logic_status.row.crc
        modified_write_path = os.path.join(abs_writable_path, str(md5))

        files = zip_utils.unzipper.extract_file(abs_file_path, modified_write_path,
                                                False, df.work_file_type, skip_ifexists=skip_ifexists)

        total_files = len(files)

        logic_status.row.total_files = total_files
    # We walk the tmp dir and add those data files to list of to do
        new_src_dir = modified_write_path
        logging.debug(
            "WALKING EXTRACTED FILES:\nsrc_dir:{0} \nworking_dir:{1}: --{2}".format(new_src_dir, df.working_path, modified_write_path))

        file_table_map = [data_files.FilesOfInterest('DATA', '.*', file_path=modified_write_path, file_name_data_regex=None,
                                                                    parent_file_id=file_id, project_name=foi.project_name)]

        # instantiate a new Datafile object that craw this new directory of extracted files
        data_files.DataFile(
            new_src_dir, db, file_table_map, parent_file_id=file_id)
    except Exception as e:
        logging.exception(f"Failed Extracting: {e}")
        logic_status.failed(e)
    
    return logic_status


# Generic code...put your custom logic above to leave room for logging activities and error handling here if any
def process(db, foi, df, logic_status):

    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
