
import logging
import os
import sys
import db_utils
import data_file_mgnt
import db_table
import zip_utils
logging.basicConfig(level='DEBUG')


'''
    File name: generate_checksum.py
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    #  This logic will extract a compressed zip file and inventory the files extracted and link it to the parent_file_id
    #  
    #  
'''


def custom_logic(db, foi, df):
    continue_processing = True
    table_name = foi.table_name
    target_schema = foi.schema_name
    file_id = df.meta_source_file_id
    skip_ifexists = (not foi.unzip_again)
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)
    abs_writable_path = os.path.join(df.working_path, df.curr_src_working_file)


# def extract_file(self, db, abs_file_path, abs_writable_path, skip_ifexists=False):
    status_dict = {}
    try:
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        row = t.get_record(db_table.db_table_def.MetaSourceFiles.id == file_id)
        md5 = None
        path = os.path.dirname(abs_file_path)
        folder_name = os.path.basename(path)
        try:
            md5 = row.crc
        except:
            logging.warning("CRC column does not exist in meta_source_file table. Please make sure you create it")
        modified_write_path = os.path.join(abs_writable_path, folder_name, str(md5))

        files = zip_utils.unzipper.extract_file(abs_file_path, modified_write_path,
                                                False, df.work_file_type, skip_ifexists=skip_ifexists)

        total_files = len(files)

        row.total_files = total_files
        t.session.commit()

    # We walk the tmp dir and add those data files to list of to do
        new_src_dir = modified_write_path
        logging.debug(
            "WALKING EXTRACTED FILES:\nsrc_dir:{0} \nworking_dir:{1}: --{2}".format(new_src_dir, df.working_path, modified_write_path))

        file_table_map = [data_file_mgnt.data_files.FilesOfInterest('DATA', '.*', file_path=modified_write_path, file_name_data_regex=None,
                                                                    parent_file_id=file_id, project_name=foi.project_name)]

        # instantiate a new Datafile object that craw this new directory of extracted files
        data_file_mgnt.data_files.DataFile(new_src_dir, db, file_table_map, parent_file_id=file_id)
    except Exception as e:
        # import time
        # print("---error occured--sleeping so you can read", e)
        # time.sleep(30)
        logging.error(e)
        status_dict['import_status'] = 'FAILED'
        status_dict['error_msg'] = 'Error During Unziping File'
        continue_processing = False

    return continue_processing


# Generic code...put your custom logic above to leave room for logging activities and error handling here if any
def process(db, foi, df):
    error_msg = None
    additional_msg = None

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.dbconn.Connection)

    return custom_logic(db, foi, df)
