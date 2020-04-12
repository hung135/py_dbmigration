from enum import Enum
import sys
import py_dbmigration.db_table as db_table
from py_dbmigration.data_file_mgnt import utils
import os, logging as lg

logging=lg.getLogger()


#enums to classify various states if a file
class WorkState(Enum):
    HAVE_MORE_WORK = 0
    NO_MORE_WORK = 1
    SLEEP = 2
    

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
    def __init__(self, db, file,file_id,appname=None):
        self.file_path=os.path.abspath(file)
        self.name=os.path.basename(file)
        self.status = FileStateEnum.RAW
        self.error_msg = None
        self.rows_inserted = None
        self.file_extracted = None
        self.table_name = None
        self.file_id=file_id
        self.db=db
        # try:
        #     self.table.close()
        # except:
        #     pass
        self.table = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles,appname or self.name)
        self.row = self.table.get_record(db_table.db_table_def.MetaSourceFiles.id == file_id)
        
        self.row.file_process_state=self.status.value
        self.commit()

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):
       
        return f"""File: {self.name}\nStatus: {self.status}\nError_msg:  {self.error_msg}\n """
         
    def refresh(self,appname=None):
        try:
            self.close()
        except:
            pass
        self.table = db_table.db_table_func.RecordKeeper(self.db, db_table.db_table_def.MetaSourceFiles,appname or self.name)
        self.row = self.table.get_record(db_table.db_table_def.MetaSourceFiles.id == self.file_id)

    def close(self):
        self.table.session.commit()
        self.table.session.close()
        self.table.close()
    def __del__(self):
     
        self.close()
    def commit(self):
        self.table.session.commit()
    def rollback(self):
        self.table.session.rollback()
    def authenticate(self):
        pass
    def processed(self,reprocess=False):
        if not self.status in [FileStateEnum.FAILED,FileStateEnum.DUPLICATE,FileStateEnum.OBSOLETE]:
            self.status=FileStateEnum.PROCESSED
        else:
            self.status=FileStateEnum.FAILED
        logging.debug(f"Processed: Reprocess: {reprocess}, Statue: {self.status.value}")
        self.row.reprocess=reprocess
        self.row.file_process_state=self.status.value
        self.commit()
        return self.status.value
    def obsolete(self):
        self.status=FileStateEnum.OBSOLETE
        self.row.file_process_state=self.status.value
        self.commit()
        return False
    def duplicate(self):
        self.status=FileStateEnum.DUPLICATE
        self.row.file_process_state=self.status.value
        self.commit()
        return False        
    def failed(self,msg,reprocess=False):
        logging.error(f"FAILED: {msg}")
        self.rollback()
        logging.error(f"FAILED: Reprocess: {reprocess}, Prev Status: {self.status.value}")
        self.status=FileStateEnum.FAILED
        
        self.row.reprocess=reprocess
        self.row.file_process_state=self.status.value
        self.row.last_error_msg=str(msg)
        self.commit()
        

        return False
        #logging.error("Data File Processing FAILED: {}".format(self.file_path))
 
    def hardfail(self,msg=None):
        #self.close()
        sys.exit("Hard Fail Initiated Data File: \n\t{}\nError Msg: {}".format(self.file_path,msg ))
    # def close(self):
    #     self.table.session.commit()
    #     self.table.session.close()
        
    def set_cfpb_load_status(self):
        pass
    def set_cfpb_error_log(self):
        pass

    def __str__(self):
        return_string="""File: {}\nStatus: {}\nError_msg:  {}\n """
        return return_string.format(self.name,self.status,self.error_msg)


class LogicState:
    # object to carry status info for prossing and import
    # tract status of logic on a file
    # only tag file record if logic fails
    
    status = None
    name = None
    error_msg = None
    continue_processing_logic=None
    file_state = None
    #logic_options = {}
    def __init__(self, file : str ,file_state : DataFileState):
         
        self.file_path=file
        self.name=os.path.basename(file)
        self.status = LogicStateEnum.INIT
        self.error_msg = None
        self.return_value = None
        self.row=file_state.row
        self.table=file_state.table
        #assert isinstance(file_state.row,db_table.db_table_def.MetaSourceFiles)
        self.file_state=file_state
        # file_state.refresh(self.name)
        
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

    #This logic has ran to completion
    def continue_to_next_logic(self,TrueFalse):
        assert isinstance(TrueFalse,bool)
        logging.info("\t\tSet Continue to Next Logic: {}".format(TrueFalse))
        self.continue_processing_logic=TrueFalse
    #This logic has ran to completion
    def completed(self):
  
        if not ( self.row.file_process_state  in ('OBSOLETE','FAILED','DUPLICATE')):
            self.status=LogicStateEnum.COMPLETE
            #self.row.file_process_state=LogicStateEnum.COMPLETE.value
            self.continue_processing_logic=True
        else:
            self.continue_processing_logic=False
            self.row.file_process_state=LogicStateEnum.FAILED.value
        if self.row.process_msg_trail is None:
            self.row.process_msg_trail=self.name[:2000]
        else:
            self.row.process_msg_trail=str(self.name +f"\n{self.row.process_msg_trail}")[:2000]
         
        self.table.session.commit()
         
    # def processed(self):
    #     if not self.status in [LogicStateEnum.FAILED]:
    #         self.status=LogicStateEnum.COMPLETE
    #     self.table.session.commit()
    
    def failed(self,msg: str):
        self.status=LogicStateEnum.FAILED
        self.continue_processing_logic=False
        
        self.file_state.failed(str(msg))
        

    def failed_continue(self,msg: str ):
        self.status=LogicStateEnum.FAILED
        self.continue_processing_logic=True
        self.file_state.failed((msg))

    def hardfail(self,msg: str ):
        self.file_state.failed(str(msg))
        sys.exit("Hard Fail Initiated for Logic File: \n\t{}".format(self.file_path))

    def __del__(self):
        assert isinstance(self.row,db_table.db_table_def.MetaSourceFiles)
        try:
             
            self.table.session.commit()
            self.table.close()
            
        except Exception as e:
 
            logging.exception(e)
    def authenticate(self):
        pass


#this is mapped to the contents of the YAML file
class FOI(object):
    
    # 2 scerios...given a path and a file pattern we walk the dir
    # gven table_name and a file regex we use it to map files from the meta
    # source to a table
    project_name = None
    regex = None
    path = None
    write_path = None
    file_path = None
    file_type= None
    table_name= None
    file_regex= None
    file_delimiter= None
    column_list= None
    column_list2 = None
    schema_name= None
    use_header= None
    has_header= None
    append_file_id= None
    file_encoding= None
    pre_action_sql= None
    post_action_sql= None
    pre_action= []
    post_action= []
    limit_rows= None
    process_logic= None
    post_process_logic = [] #these logic will get executed regardless
    pre_process_scripts = []
    post_process_scripts = []
    reprocess=False 
    extract_file_name_data = None
    format_extracted_date = None
    file_name_data_regex = None
    table_name_extract = None
    unzip_again = None
    header_row = None
    new_delimiter = None
    convert_table_name_snake_case = None
    encoding = None
    mapping = None
    def __init__(self,level1,mapping=[]):

        for key in level1:
            if str(level1[key])=='None':
                setattr(self, key, None)
            else:
                setattr(self, key, level1[key])
                #print("----foi key ",key,level1[key])

        for key in mapping:
            if str(mapping[key])=='None':
                setattr(self, key, None)
            elif (key)=='process_logic':
                self.process_logic=self.process_logic + (mapping[key] or []) 
            else:
                setattr(self, key, mapping[key])

        #ensure variable data types are proper here so we don't have to massage it else wheres
        self.file_path = self.path or self.file_path
        if not (self.file_path.lower().startswith('s3') or self.file_path.lower().startswith('switchboard@')):
            self.file_path = os.path.abspath(self.file_path)
            
         
        self.regex = self.regex or self.file_regex
        self.file_name_data_regex = self.extract_file_name_data or self.file_name_data_regex
        self.encoding = self.file_encoding or self.encoding
        self.limit_rows = self.limit_rows 
        self.header_row = self.header_row or 0
        if self.column_list is not None:
            self.column_list =  self.column_list.replace(' ', '').replace('\n', '').split(',')
        if self.column_list2 is not None:
            self.column_list2 = self.column_list2.replace(' ', '').replace('\n', '').split(',')
        print(self.pre_process_scripts)
    #set runtime values for any objects
    def render_runtime_data(self,df):
        obj=self
        self_attributes=[a for a in dir(obj) if not a.startswith('__') and not callable(getattr(obj,a))]
         

        for a in self_attributes:
            field=getattr(self,a)
            if isinstance(field,str):
                field=utils.inject_frame_work_data(field,self,df)
                setattr(self,a,field)
        if self.pre_action is not None:
            assert isinstance(self.pre_action,list)
            for sql in self.pre_action:
                if isinstance(sql,str):
                    field=utils.inject_frame_work_data(sql,self,df)
                    setattr(self,a,field)
        if self.post_action is not None:           
            assert isinstance(self.post_action,list)         
            for sql in self.post_action:
                if isinstance(sql,str):
                    field=utils.inject_frame_work_data(sql,self,df)
                    setattr(self,a,field)

    def __str__(self):
         
        string_result={
            'project_name':self.project_name,
            'regex_pattern': self.regex,
            'file_path':self.path,
            'current_file':self.file_path,
            'pre_process_scripts': self.pre_process_scripts,
            'post_process_scripts': self.post_process_scripts

        }
         
        return str(string_result)
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __repr__(self):

        return self.__str__()

    def __del__(self):
        pass

  
#this will be decprecated one day as you can see the why in the __init__ function
class FilesOfInterest(FOI):
    
    # 2 scerios...given a path and a file pattern we walk the dir
    # gven table_name and a file regex we use it to map files from the meta
    # source to a table
    write_path=None
    extract_file_name_data = None
    format_extracted_date = None
    file_path = None # the direction not the FQN of the file...
    def __init__(self, file_type, file_regex, table_name=None, file_delimiter=None, column_list=None, schema_name=None,
                 use_header=False, has_header=True, quoted_header=False, folder_regex=None, append_file_id=False, append_column_name='file_id',
                 file_name_data_regex=None, file_path=None, parent_file_id=0, insert_option=None, encoding='UTF8',
                 append_crc=False, limit_rows=None, header_row_location=0,  
                 new_delimiter=None, dataset_name=None, redaction_file=None,
                 upsert_function_name=None, import_method=None, unzip_again=False, pre_action_sql=None,
                 post_action=None, pre_action=None, process_logic=None, project_name='Default',
                 table_name_extract=None, reprocess=False, yaml=None,mapping=None):
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
        return self.__str__()

    

    def __del__(self):
        pass

    def authenticate(self):
        pass

   