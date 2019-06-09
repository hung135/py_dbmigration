from enum import Enum
import os, sys
import py_dbmigration.db_table as db_table

#enums to classify various states if a file
class FileStateEnum(Enum):
    RAW = 'RAW'
    PREACTION = 'PREACTION' #logic(sql) before custom logic starts to run
    POSTACTION = 'POSTACTION' #logic after all custom logic has ran
    IMPORTED = 'IMPORTED'
    PROCESSING = 'PROCESSING' # file is in the middle of getting processed
    PROCESSED = 'PROCESSED' # file has completed process
    OBSOLETE = 'OBSOLETE' #Data is render Obsolete when source delivers new complete set
    DUPLICATE = 'DUPLICATE' #duplicate data file based on file hash
    FAILED = 'FAILED'  #fail to import file
    HARDFAIL = 'HARDFAIL' #syntax and such errors WILL exit program
#enums to classify various states if a custom logic
class LogicStateEnum(Enum):
    INIT = 'INIT' 
    RUNNING = 'RUNNING' #currenttly 
    COMPLETE = 'COMPLETE' #Logic ran to termination without errors
    FAILED = 'FAILED' #errors with the data
    HARDFAIL = 'HARDFAIL' #syntax and such errors WILL exit program

#State of the data file
class DataFileState:
    # object to carry status info for prossing and import
    status = FileStateEnum.RAW
    name = None
    rows_inserted = 0
    error_msg = None
    table_name = None
    continue_processing_logic=False
    file_id = None
    def __init__(self, db, file,file_id):
        self.file_path=file
        self.name=os.path.basename(file)
        self.status = FileStateEnum.RAW
        self.error_msg = None
        self.rows_inserted = None
        self.file_extracted = None
        self.table_name = None
        self.file_id=file_id
        self.table = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        self.row = self.table.get_record(db_table.db_table_def.MetaSourceFiles.id == file_id)
        
        self.row.file_process_state=self.status.value
        self.table.session.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        return_string="""File: {}\nStatus: {}\nError_msg:  {}\n """
        return return_string.format(self.name,self.status,self.error_msg)

    

    def __del__(self):
        self.table.session.commit()
        self.table.session.close()

    def authenticate(self):
        pass
    def processed(self):
        self.status=FileStateEnum.PROCESSED
        self.row.file_process_state=self.status.value
        self.table.session.commit()
    def obsolete(self):
        self.status=FileStateEnum.PROCESSED
        self.row.file_process_state=self.status.value
        self.table.session.commit()        
    def failed(self,msg):
        self.status=FileStateEnum.FAILED
        self.row.file_process_state=self.status.value
        self.row.last_error_msg=msg
        self.table.session.commit()
        #logging.error("Data File Processing FAILED: {}".format(self.file_path))
 
    def hardfail(self,msg=None):
        self.table.session.commit()
        self.table.session.close()
        sys.exit("Hard Fail Initiated Data File: \n\t{}\n{}".format(self.file_path,msg or ''))
    def close(self):
        self.table.session.commit()
        self.table.session.close()
        
    def set_cfpb_load_status(self):
        pass
    def set_cfpb_error_log(self):
        pass

    def __str__(self):
        return_string="""File: {}\nStatus: {}\nError_msg:  {}\n """
        return return_string.format(self.name,self.status,self.error_msg)


class LogicState:
    # object to carry status info for prossing and import
    status = None
    name = None
    error_msg = None
    continue_processing_logic=None
    file_state = None
    
    def __init__(self, file,file_state):
        self.file_path=file
        self.name=os.path.basename(file)
        self.status = LogicStateEnum.INIT
        self.error_msg = None
        self.return_value = None
        self.row=file_state.row
        self.table=file_state.table
        #assert isinstance(file_state.row,db_table.db_table_def.MetaSourceFiles)
        self.file_state=file_state
        if self.row.file_process_state==FileStateEnum.RAW.value:
            self.row.file_process_state=FileStateEnum.PROCESSING.value
            self.table.session.commit()
        
   
    def __str__(self):
        return_string="""Logic: {}\nStatus: {}\nError_msg:  {}\n FileState: {}"""
        return return_string.format(self.name,self.status,self.error_msg, self.file_state.status)

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        return_string="""Logic: {}\nStatus: {}\nError_msg:  {}\n FileState: {}"""
        return return_string.format(self.name,self.status,self.error_msg, self.file_state.status)
    
    #This logic has ran to comletion
    def completed(self):
        if not self.status==LogicStateEnum.FAILED:
            self.status=LogicStateEnum.COMPLETE
            self.continue_processing_logic=True
        if self.row.process_msg_trail is None:
            self.row.process_msg_trail=self.name
        else:
            self.row.process_msg_trail=self.name +"\n{}".format(self.row.process_msg_trail)
         
        self.table.session.commit()
    def processed(self):
        self.status=LogicStateEnum.COMPLETE
        self.row.file_process_state=self.status.value
        self.table.session.commit()
    
    def failed(self,msg):
        self.status=LogicStateEnum.FAILED
        self.continue_processing_logic=False
        self.row.file_process_state=self.status.value
        self.row.last_error_msg=msg
        self.table.session.commit()

    def failed_continue(self,msg):
        self.status=LogicStateEnum.FAILED
        self.continue_processing_logic=True
        self.row.file_process_state=self.status.value
        self.row.last_error_msg=msg
        self.table.session.commit()

    def hardfail(self,msg=None):
        self.file_state.fail()
        sys.exit("Hard Fail Initiated for Logic File: \n\t{}".format(self.file_path))

    def __del__(self):
        assert isinstance(self.row,db_table.db_table_def.MetaSourceFiles)
        self.row.last_error_msg=self.error_msg or self.row.last_error_msg
        self.table.session.commit()
        #self.table.session.close()

    def authenticate(self):
        pass

    def close(self):
        pass

class FilesOfInterest:
    
    # 2 scerios...given a path and a file pattern we walk the dir
    # gven table_name and a file regex we use it to map files from the meta
    # source to a table

    def __init__(self, file_type, file_regex, table_name=None, file_delimiter=None, column_list=None, schema_name=None,
                 use_header=False, has_header=True, quoted_header=False, folder_regex=None, append_file_id=False, append_column_name='file_id',
                 file_name_data_regex=None, file_path=None, parent_file_id=0, insert_option=None, encoding='UTF8',
                 append_crc=False, limit_rows=None, header_row_location=0,  
                 new_delimiter=None, dataset_name=None, redaction_file=None,
                 upsert_function_name=None, import_method=None, unzip_again=False, pre_action_sql=None,
                 post_action=None, pre_action=None, process_logic=None, project_name='Default',
                 table_name_extract=None, reprocess=True, yaml=None,mapping=None):
        self.yaml = yaml
        self.mapping = mapping
        # avoid trying to put any logic here
        self.regex = file_regex
        self.folder_regex = folder_regex
        self.table_name = table_name
        self.schema_name = schema_name
        if column_list is not None:
            self.column_list = column_list.replace(' ', '').replace('\n', '').split(',')
        else:
            self.column_list = None
        
        self.file_delimiter = file_delimiter
        self.use_header = use_header
        self.has_header = has_header
        self.quoted_header = quoted_header
        self.import_method = import_method
        self.append_file_id = append_file_id
        self.append_column_name = append_column_name
        self.file_type = file_type
        self.file_name_data_regex = file_name_data_regex
        self.append_crc = append_crc

        if file_path is not None and not(file_path[:5]=='s3://'):
            self.file_path = file_path = os.path.abspath(file_path)
        else:
            self.file_path = file_path
        self.parent_file_id = parent_file_id
        self.insert_option = insert_option
        self.encoding = encoding
        self.total_files = 0

        self.encoding = encoding
        self.current_working_abs_file_name = None
        self.limit_rows = limit_rows
        self.header_list_returned = None
        self.header_added = None
        # self.start_row = start_row
        self.header_row = header_row_location or 0
        
        self.new_delimiter = new_delimiter
        self.dataset_name = dataset_name
        self.redaction_file = redaction_file
        self.upsert_function_name = upsert_function_name
        self.unzip_again = unzip_again
        self.pre_action_sql = pre_action_sql
        # list of sql to execute prior or post import of the file
        self.post_action = post_action
        self.pre_action = pre_action
        self.process_logic = process_logic
        self.project_name = project_name
        self.table_name_extract = table_name_extract
        self.reprocess = reprocess

    
    def __str__(self):
        string_result={
            'project_name':self.project_name,
            'regex_pattern': self.regex,
            'file_path':self.file_path,
            'current_file':self.current_working_abs_file_name

        }
         
        return str(string_result)
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
        pass

    

    def __del__(self):
        pass

    def authenticate(self):
        pass

    def close(self):
        pass