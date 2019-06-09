
import logging
import os
import sys
import py_dbutils.parents as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
 
import py_dbmigration.db_table as db_table
import py_dbmigration.zip_utils as zip_utils

'''
    File name: generate_checksum.py
    Author: Hung Nguyen
    Date created: 5/22/2019
    Date last modified: 5/22/2019
    Python Version: 3.6
    Descripton:
    #  This logic will extract a compressed zip file and inventory the files extracted and link it to the parent_file_id
    #  
    #  
'''


def custom_logic(db, foi, df,logic_status):
    logic_status=data_file_mgnt.data_files.Status(status='Begin Custom Logic {}'.format(__file__))
    logic_status.continue_processing = True
    
    table_name = foi.table_name
    #target_schema = foi.schema_name
    file_id = df.meta_source_file_id
    #skip_ifexists = (not foi.unzip_again)
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)
    abs_writable_path = os.path.join(df.working_path, df.curr_src_working_file)
    import py_dbutils.rdbms.msaccess as msaccess
     
# def extract_file(self, db, abs_file_path, abs_writable_path, skip_ifexists=False):
     
    try:
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        row = t.get_record(db_table.db_table_def.MetaSourceFiles.id == file_id)
        md5 = None
        path = os.path.dirname(abs_file_path)
        #folder_name = os.path.basename(path)
        try:
            md5 = row.crc
            
        except:
            warning_msg = "CRC column does not exist in meta_source_file table. Please make sure you create it"
            logging.warning(warning_msg)
             
        modified_write_path = os.path.join(abs_writable_path,  str(md5))
         
        files = []    
        accessdb=msaccess.DB(abs_file_path)
    
        for table_name in accessdb.get_all_tables():
            logging.info("\t\tExtracting MSACCESS Table: {}".format(table_name))
            os.makedirs(modified_write_path,exist_ok=True)
            extracted_file_name="{}.csv".format(table_name)
            extracted_file_fqn=os.path.join(modified_write_path,extracted_file_name)
             
            accessdb.query_to_file(sql="select * from {}".format(table_name),file_path=extracted_file_fqn, file_format='CSV', 
                        header=True)
            #accessdb.query_to_file(file_path=csv_file_path, sql='select * from tblEmployees', file_format='CSV', header=y)
            files.append(extracted_file_name)
             
        total_files = len(files)

        row.total_files = total_files
        t.session.commit()

    # We walk the tmp dir and add those data files to list of to do
        new_src_dir = modified_write_path
        logging.debug(
            " EXTRACTED From ACCESS FILES:\nsrc_dir:{0} \nworking_dir:{1}: --{2}".format(new_src_dir, df.working_path, modified_write_path))

        file_table_map = [data_file_mgnt.data_files.FilesOfInterest('DATA', '.*', file_path=modified_write_path, file_name_data_regex=None,
                                                                    parent_file_id=file_id, project_name=foi.project_name)]

        # instantiate a new Datafile object that craw this new directory of extracted files
        data_file_mgnt.data_files.DataFile(new_src_dir, db, file_table_map, parent_file_id=file_id)
    except Exception as e:
        # import datetime
        # print("---error occured--sleeping so you can read", e)
         
        logging.error(e)
        logic_status.status = 'FAILED'
        logic_status.error_msg = 'Error During Extracting Files to CSV: {}'.format(e)
        logic_status.continue_processing = False
        return logic_status

    return logic_status 


# Generic code...put your custom logic above to leave room for logging activities and error handling here if any
def process(db, foi, df):
    
    

    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
