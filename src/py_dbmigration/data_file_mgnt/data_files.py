import re
 
#import zip_utils
import socket
import datetime
import copy
import pandas as pd
#import db_logging
import py_dbmigration.db_table as db_table
from py_dbmigration.db_table.pid_worker import PidManager
# mport db_table.db_table_func
import yaml
import sys
import time

import py_dbmigration.migrate_utils as migrate_utils
import py_dbutils.parents as db_utils
from py_dbmigration.data_file_mgnt import utils 
from py_dbmigration.data_file_mgnt.state import DataFileState, FOI,LogicState, WorkState
import os
import logging as log

logging = log.getLogger()
logging.setLevel(log.DEBUG)





# given 2 data frame this will find all the records do not exist in the
# right data frame
# def dataframe_diff_left(df1, df2, column_name):
#     result = pd.merge(df1, df2, on=column_name, how='left', indicator=True)
#     return result.query("_merge =='left_only'")


# def diff_list(list1, list2):
#     delta = []
#     l1 = set(list1)
#     l2 = set(list2)

#     for i in list1:
#         if i not in list2:
#             delta.append(i)
#     return l1 - l2


# object to carry status info for prossing and import


# Struct used to group parameters to define files of interests


def get_matching_yaml(file_name, foi_list):
    import copy
    print("--------",file_name)
    for foi in foi_list:
        # print(i.regex)
        # if i.table_name is not None:
        assert isinstance(foi, FOI)

        if re.match(foi.file_regex, file_name, re.IGNORECASE):
            print(foi)
            logging.debug(
                "File->Table mapping found: {} {}".format(foi.file_type, foi.file_regex))
            return copy.copy(foi)
    return None


def convert_to_sql(instring):
    sqlstring = instring.replace("'", "\'")
    sqlstring = sqlstring.replace('"', '\"')
    return sqlstring


# Main class that will handle the lifecycle of a data file:
#       acquisition from the source directory
#       extracted, processed, tagged w/ file_id, checksum every row
#       imported into target database table
#       logs any error
#           Pandas will import in chunks
#           Client side CopyCommand All or Nothing

class DataFile:
    #COMPRESSED_FILE_TYPES = ['ZIP', 'GZ', 'TAR']
    #    SUPPORTED_DATAFILE_TYPES = ['DATA', 'CSV', 'DAT', 'XLSX', 'TXT', 'XLS', 'MDB']
    pidManager = None
    working_path = None
    full_file_path = None
    db = None
    file_pattern_list = None
    current_file_pattern = None
    curr_table_row_count = None
    # update_query = """Update meta_source_files
    #                    set file_process_state='{0}',
    #                        process_end_dtm = now()
    #                    WHERE  file_name='{1}' and
    #                    """
    current_file_state = None
    file_name_data_regex = None
    file_id = None
    #claim_size = 1 #number of files to fetch each time

    def get_curr_table_row_count(self,fqn_table_name):
        current_table_row_count=0
        sql = f'select count(1) from {fqn_table_name}'
        logging.debug(f'Counting table, This could take sometime: {fqn_table_name}')
        try:
            current_table_row_count,=self.db.get_a_row(sql)
        except Exception as e:
            logging.exception(e)
        logging.info(f'Counting table, Returned: {current_table_row_count} rows')
        return current_table_row_count


    def __init__(self, working_path, db, foi_list, parent_file_id=0,compressed_file_type=None,claim_size=1):
         
        self.claim_size=claim_size
        self.file_id_list=[]
        logging.debug(f'Claim Size; {self.claim_size}')
        if not self.pidManager:
            
            self.pidManager=PidManager(db,'dfm','logging','pidworker',False)
        curr_path = (os.path.dirname(__file__))

        with open(os.path.join(curr_path, 'logic_sql.yml'), 'r') as f:
            from yaml import Loader
            self.sql_yaml = yaml.load(f, Loader=Loader)

        self.load_status_msg = None
        self.parent_file_id = parent_file_id
        self.db = db

        self.project_list = []
        for p in foi_list:
             
            self.project_list.append(p.project_name)
            if p.write_path is not None:
                
                self.working_path =  p.write_path
        self.project_list = list(set(self.project_list))
        self.working_path = working_path or self.working_path or os.getcwd()
        self.working_path = os.path.abspath(self.working_path)
        self.compressed_file_type = compressed_file_type
        # self.file_pattern_list = file_pattern_list
        self.source_file_path = None

        self.curr_src_working_file = None
        self.curr_pid = None
        self.host = None
        # the  files found walking the source directory based on the regex
        # passed in
        self.source_files = None
      
        self.files = None  # The contents of the zip files for the last file unzipped
        self.curr_pid = os.getpid()
        self.host = socket.gethostname()
        self.embedded_id = 0
        self.work_file_type = 0
        self.total_files = 0
        self.curr_file_success = False
        #self.FilesOfInterest = None
        self.total_data_file_count = 0
        self.foi_list = foi_list
        self.rows_inserted = 0  # initialzed in constructor
        
        # self.table_file_regex = \
        # self.put_foi_to_db(db, foi_list)

        # take each pattern and walks the directory

        for files_of_interest in self.foi_list:
            if files_of_interest.file_path is not None:
                
                #assert isinstance(files_of_interest, FilesOfInterest)
                file_path = files_of_interest.file_path[:5]
                if file_path.upper() == 'S3://':
                    prog = re.compile('S3://.*')

                    if not prog.match( files_of_interest.file_path.upper()):
                        raise Exception ("Bad Format S3","S3://<url>")

                    logging.info("Walking AWS s3")
                    self.FilesOfInterest = self.walk_s3(
                        files_of_interest)
                elif 'switchboard@' in files_of_interest.file_path.lower():
                    prog = re.compile('switchboard@.*:.*:.*') 
                    if not prog.match( files_of_interest.file_path.lower()):
                        raise Exception ("Bad Format Switchboard","switchboard@<project_name>:<dbname>:<hostname>")
                    import py_dbutils.rdbms.postgres as dbconn 
                    switch_db_hostname = files_of_interest.file_path.split(':')[-1]
                    switch_db_name = files_of_interest.file_path.split(':')[1] 
                    switch_project_name = files_of_interest.file_path.split(':')[0].split('@')[-1]
                    logging.debug(f"Connecting to Switchboard : {switch_db_name} : {switch_db_hostname}")
                   
                    sw_db=dbconn.DB(host=switch_db_hostname,dbname=switch_db_name)
                      
                    logging.debug(f"Fetching from Switchboard: {switch_project_name}:{switch_db_name}:{switch_db_hostname}")
                    rs,_=sw_db.query(f"Select outgoing_path, id from switchboard.switchboard_history where upper(project_name)=upper('{switch_project_name}') and state='M' ")
                     
                    #file_list=get_switch_board_file(files_of_interest.project_name)
                    for row,id in rs:
                        file_path=row
                             
                        # file_list.append(file_path)
                        # id_list.append(id)
                          
                        switchboard_foi=(self.foi_from_list(files_of_interest,[file_path]))
                        switchboard_foi.file_type= file_path.split('.')[-1].upper()
                        #putting each file into meta_source
                        self.insert_working_files(  db, switchboard_foi )
                        sw_db.execute(f"update switchboard.switchboard_history set state='C' where id={id}")
                else:
                    if os.path.isdir(files_of_interest.file_path):
                        if files_of_interest.file_regex:
                            
                            self.FilesOfInterest = self.walk_dir( files_of_interest)
                            self.FilesOfInterest.parent_file_id = self.file_id

                    else:
                        logging.error("Directory from Yaml does not exists: {}".format(
                            files_of_interest.file_path))
                        sys.exit(1)
                    

                    if  ( self.FilesOfInterest):
                        
                        self.insert_working_files(
                            db, self.FilesOfInterest, self.parent_file_id)

    def init_db(self):
        appname = os.path.basename(__file__)+"init_db"
        t = db_table.db_table_func.RecordKeeper(
            self.db, db_table.db_table_def.MetaSourceFiles, appname=appname)
        t.close()
 
    def validate_regex(self, regex):
        compiled_regex = None

        try:
            compiled_regex = re.compile(regex, re.IGNORECASE)
        except Exception as e:
            logging.exception(r"Invalid Regex: {}, Exception:{}".format(regex, e))
            raise
        else:
            pass
        finally:
            pass
        return compiled_regex

    # id_regex is used to extract some meaning data FROM the name of the file like a year or month or date
    # that id gets stored with the meta data about the file to later use

    def insert_working_files(self, db, foi, parent_file_id=0):
        assert  isinstance( foi, FOI)
        appname = os.path.basename(__file__)+"insert_working_files"
        t = db_table.db_table_func.RecordKeeper(
            db, db_table.db_table_def.MetaSourceFiles, appname=appname)
         

        for walked_filed_name,file_name_data in zip(foi.file_list,foi.file_name_data_list):
         

            if 's3://' in foi.file_path:
                full_file_path = walked_filed_name
            else:
                full_file_path = os.path.join(
                    foi.file_path, walked_filed_name)
            file_name = os.path.basename(full_file_path)
            file_path = os.path.dirname(full_file_path)



            # If the file already exists in the database we don't need to
            # insert again
            sql= "select count(1) from logging.meta_source_files where file_name='{}' and file_path='{}' and project_name='{}'".format(
                    file_name, file_path, foi.project_name)
            logging.debug(f"Checking File Exist: {sql}")
            file_found, = db.get_a_row(sql)
            
          

            if file_found == 0:
                logging.debug(f"New file found: {full_file_path}")
                
                v_file_type = foi.file_type
                if foi.file_type == 'DATA':
                    v_file_type = file_name.split(".")[-1].upper()
                
                row = db_table.db_table_def.MetaSourceFiles(file_path=file_path,
                                                            file_name=file_name,
                                                            file_name_data=file_name_data,
                                                            file_type=v_file_type,
                                                            parent_file_id=parent_file_id,
                                                            project_name=foi.project_name
                                                            )
                t.add_record(row)
                t.session.commit()

    def dump_delimited_file(self, db, file_name, delimiter):
        shell_command = """psql -c "copy data_table FROM '{0}}' WITH DELIMITER AS '{1}' CSV QUOTE AS '"' """
        shell_command.format(file_name, delimiter)

    @staticmethod
    def reset_meta_table(db, option='FAILED', where_clause='1=1'):
        curr_path = (os.path.dirname(__file__))
        sql_yaml = None
        with open(os.path.join(curr_path, 'logic_sql.yml'), 'r') as f:
            from yaml import Loader
            sql_yaml = yaml.load(f, Loader=Loader)

        if option.upper() == 'ALL':
            db.execute(
                sql_yaml['sql_update_ALL_meta_source'].format(where_clause))
        if option.upper() == 'FAILED':
            logging.debug("RESET META DATA FAILED IMPORTS:")
            db.execute(
                sql_yaml['sql_update_FAILED_meta_source'].format(where_clause))
        if option.upper() == 'RAW':
            logging.debug("RESET META DATA RAW IMPORTS:")
            db.execute(
                sql_yaml['sql_update_RAW_meta_source'].format(where_clause))
        if option.upper() == 'DATA':
            logging.debug("RESET META DATA   IMPORTS:")
            db.exeucte(
                sql_yaml['sql_update_RAW_meta_source'].format(where_clause))

    @staticmethod
    def walk_s3(foi,  db=None):
        import boto3

        regex = None
        try:
            regex = re.compile(foi.file_regex)
        except Exception :
            logging.exception(
                "Bad Regex Pattern for Walking Directory: '{}'".format(foi.file_regex))
            return None

        s3 = boto3.resource('s3')
        split_url = foi.file_path.replace('s3://', '').split('/')
        bucket_name = split_url[0]

        bucket = s3.Bucket(bucket_name)

        files_list = []
        # for o in bucket.objects.filter(Delimiter='/'):
        #    files_list.append(o.key)

        #s3 = boto3.client("s3")
        #all_objects = s3.list_objects(Bucket = bucket_name)
        #contents = all_objects.get('Contents')
        filter1 = 's3://'+bucket_name+'/'

        files = list(bucket.objects.filter(
            Prefix=foi.file_path.replace(filter1, '')))
        # print(len(files))
        for item in files:

            file_name = item.key
            files_list.append(filter1+file_name)

            # print(item.get('Key'))
            # print("\n")
        match_list = list(filter(regex.match, files_list))
        foi.file_list = match_list
        return foi

    @staticmethod
    def foi_from_list(foi, file_path_list,file_type=None):
        files_list=[]
        file_name_data_list=[]
        
        for file_path in file_path_list:
          
            extracted_data=''
            files_list.append(file_path)
            if foi.file_name_data_regex is not None:
                re_extracted_data=re.search(foi.extract_file_name_data,file_path) or ''
                if re_extracted_data:
                    extracted_data=re_extracted_data.group()

            file_name_data_list.append(extracted_data)
        

        foi.file_list = files_list
        foi.file_name_data_list = file_name_data_list
         
        return foi
    @staticmethod
    def walk_dir(foi):
        """Walks a directory structure and returns all files that match the regex pattern
        :rtype: FOI
        """
        assert isinstance(foi, FOI)
        file_path = foi.file_path
        logging.debug("Walking Directory: '{}' : Search Pattern: {}".format(
            file_path, foi.file_regex))

        regex = None
        try:
 
            regex = re.compile(foi.file_regex)
        except Exception as e:
            logging.exception(
                "Bad Regex Pattern for Walking Directory: '{}' \n{}".format(foi.file_regex, e))
            raise

        files_list = []
        file_name_data_list = []

        for root, _, files in os.walk(file_path, topdown=True):
            # print(root)

            for x in files:
                # print('\t\t{}'.format(x))
                extracted_data=''
                files_list.append(os.path.join(root, x))
                if foi.file_name_data_regex is not None:
                    xx=re.search(foi.extract_file_name_data,x) or ''
                    if xx:
                        extracted_data=xx.group()

                file_name_data_list.append(extracted_data)
        # logging.debug("Done Walking Directory:")
        match_list = list(filter(regex.match, files_list))

        foi.file_list = match_list
        foi.file_name_data_list = file_name_data_list

        return foi

    def match_regex(self, regex, folder_regex=None):
        t = False

        if re.match(regex, self.curr_src_working_file, re.IGNORECASE):
            if folder_regex is not None:
                if re.match(folder_regex, self.source_file_path, re.IGNORECASE):
                    t = True

            else:
                t = True
        return t

    def cleanup_files(self):
        if self.curr_file_success:
            if self.work_file_type == 'DATA':
                logging.debug("Deleting File:{0}".format(
                    self.source_file_path + '/' + self.curr_src_working_file))
                os.remove(self.source_file_path + '/' +
                          self.curr_src_working_file)
                if not os.listdir(self.source_file_path):
                    os.rmdir(self.source_file_path)

    def insert_prepend(self, orgfile, newfile, string_data):
        with open(orgfile, 'r') as f:
            with open(newfile, 'w') as f2:
                f2.write(string_data + f.read())

    @staticmethod
    def list_current_work(db:db_utils.DB, host=None):
        assert isinstance(db, db_utils.DB)
        if host is not None:
            logging.info(db.query("""SELECT file_name, current_worker_host FROM meta_source_files,id
                        WHERE  current_worker_host is not null and current_worker_host_pid is not null
                        ORDER BY process_start_dtm"""))
        else:
            logging.info(db.query("""SELECT file_name, current_worker_host FROM meta_source_files,id
                        WHERE  current_worker_host like '%{}%' and current_worker_host_pid is not null
                        ORDER BY process_start_dtm""".format(host)))

    # process_error HAS to equal 'success' to be marked as process
    # todo handle error when file is not longer located in directory because
    # it was moved

    def release_file_lock(self, db, file_id):
        update_sql = f"""UPDATE logging.meta_source_files set process_end_dtm=now()
        where id={file_id}"""
        assert isinstance(db, db_utils.DB)
        db.execute(update_sql)

    def set_work_file_status(self, db, file_id, status, error_msg=None):
        if error_msg is None:
            error_msg = 'last_error_msg'
        else:
            error_msg = "'{}'".format(error_msg)

        # update_sql = """UPDATE logging.meta_source_files set file_process_state='{}'
        # ,last_error_msg='{}'
        # where id={} and file_process_state not in ('OBSOLETE','DUPLICATE')""".format( status, error_msg, file_id )

        # assert isinstance(db, db_utils.DB)
        # db.execute(update_sql)
    def pop_row(self):
         
        row=self.file_id_list.pop()
       
        self.reset_stat()
        self.curr_src_working_file = row["curr_src_working_file"]
        self.source_file_path = row["source_file_path"]
        self.embedded_id = row["embedded_id"]
        self.work_file_type = row["work_file_type"]
        self.total_files = row["total_files"]
        self.file_size = row["file_size"]
        self.row_count = row["row_count"]
        self.full_file_path = os.path.join(self.source_file_path,self.curr_src_working_file)
    
        self.file_id=row["file_id"]

      

    def reset_stat(self):
        self.rows_inserted = 0
       
        self.total_data_file_count = 0
        self.curr_file_success = False  # reset status of file
        self.rows_inserted = 0
        self.crc = None

    # @migrate_utils.static_func.timer
    def claim_work(self, db):
        logging.debug(f'Claim Size: {self.claim_size}')
        assert isinstance(db, db_utils.DB)
        assert isinstance(self.foi_list, list)
         
        if len(self.file_id_list)==0:
 
            x = set(self.project_list)
           
            project_list = (','.join("'" + item + "'" for item in x))
            appname = os.path.basename(__file__)+"get_work"
            sqlAlcTable = db_table.db_table_func.RecordKeeper(
                db, db_table.db_table_def.MetaSourceFiles, appname=appname)

            # to ensure we lock 1 row to avoid race conditions

            sql = self.sql_yaml['sql_get_work'].format(
                db_table.db_table_def.MetaSourceFiles.DbSchema,
                self.host, self.curr_pid, project_list).replace('limit 1',f'limit {self.claim_size}')
            logging.debug(f"Claiming work SQL: {sql}")   
            sqlAlcTable.engine.execute(sql)
            logging.debug(f"Work Claimed")
            sqlAlcTable.session.commit()
             
            rows = sqlAlcTable.get_records(db_table.db_table_def.MetaSourceFiles.current_worker_host == self.host,
                            db_table.db_table_def.MetaSourceFiles.current_worker_host_pid == self.curr_pid,
                            db_table.db_table_def.MetaSourceFiles.process_end_dtm == None)
            logging.debug("Pulling in Work meta in va SQLAlchemy")
             
            if len(rows)==0:
                 
                logging.info("\tNo Work Left, Checking Unzip in Progress")
                sql = self.sql_yaml['sql_any_proc_still_unzipping']

                unzipping_count=1
                sqlAlcTable.close() #closing sqlalchemy so we don't lock while sleeping
                while(unzipping_count>0):
                    unzipping_count,=db.get_a_row(sql)
                    if (unzipping_count)>0:
                        logging.info("\tStill Got someone Unzipping going back to sleep")
                        time.sleep(10)
                        return WorkState.SLEEP
                    else:
                        return WorkState.NO_MORE_WORK
            else:

                for row in rows:
                     
                    row_dict={
                    "curr_src_working_file":row.file_name,
                    "source_file_path": row.file_path,
                    "embedded_id":row.file_name_data,
                    "work_file_type": row.file_type,
                    "total_files": row.total_files,
                    "file_size": row.file_size,
                    "row_count": row.total_rows,
                    "file_id":row.id}

                    self.file_id_list.append(row_dict)
            sqlAlcTable.close()
        logging.debug(f'{len(self.file_id_list)} Files Left before ReQuery')
        self.pop_row()
        return WorkState.HAVE_MORE_WORK
    def do_pre_process_scripts(self,db,foi_list):
         
        self.pidManager.checkin('pre_process_scripts','START')
        scripts=[] 
        for foi in foi_list:
             
            #just take the last instance
            scripts = foi.pre_process_scripts
        utils.loop_through_scripts(db,scripts)
        self.pidManager.checkin('pre_process_scripts','DONE')
        
    def do_post_process_scripts(self,db,foi_list):
        
        scripts=[] 
        self.pidManager.checkin('post_process_scripts','START')
        #take any since they are all supposed to be the same
        for foi in foi_list:
            #just take the last instance
            scripts = foi.post_process_scripts
        utils.loop_through_scripts(db,scripts)
        self.pidManager.checkin('post_process_scripts','DONE')

    # Do work will query the meta source table for a record
    # It will stamp that record with this pid and ip address
    # When it is done with the processing of the record it we stamp the process_end_dtm
    # signifying the file has been processed

    def do_work(self, db, cleanup=True, limit_rows=None,   vacuum=True, chunksize=10000, skip_ifexists=False):

        # iterate over each file in the logging.meta_source_files table
        # get work will lock 1 file and store the id into meta_source_file_id
        # inside this instance 
         
        self.do_pre_process_scripts(db,self.foi_list)
        get_work_status=WorkState.HAVE_MORE_WORK
        
        while get_work_status in [WorkState.SLEEP, WorkState.HAVE_MORE_WORK]:
            
            
            get_work_status=self.claim_work(db)
        
            self.pidManager.checkin('BEGIN','START',claim_size=len(self.file_id_list))
             
            self.pidManager.getwork(self.file_id)
            if get_work_status == WorkState.HAVE_MORE_WORK:
                try:
                    full_file_name = os.path.join(
                        self.source_file_path, self.curr_src_working_file)
                    foi = get_matching_yaml(full_file_name, self.foi_list)
                    self.current_file_state = DataFileState(self.db, os.path.join(
                    self.source_file_path, self.curr_src_working_file), self.file_id)
                    if foi is not None:
                        

                        # self.work_file_type in self.SUPPORTED_DATAFILE_TYPES:
                        logging.debug(
                            "->Processing file_id: {}: --> {}".format(self.file_id, self.curr_src_working_file))
                        logging.debug(
                            "->Path: \n\t\t{0}\n\t\t{1}".format(self.source_file_path, full_file_name))

                        # self.set_work_file_status(
                        #     db, self.meta_source_file_id, 'Processing Started', '')
                        utils.process_logic(foi, db, self)
                        

                    else:
                        logging.error('No Matching Regex Found for this file: {}'.format(full_file_name))
                        logic_status = LogicState('NOREGEX', self.current_file_state)
                        assert isinstance(logic_status,LogicState)
                        logic_status.failed('No Matching REGEX Found in yaml')
                        # self.set_work_file_status(
                        #     db, self.meta_source_file_id, 'FAILED', 'No Matching REGEX Found in yaml')
                    self.current_file_state.table.close()
                    self.release_file_lock(db, self.file_id)
                    
                    if cleanup:
                        self.cleanup_files()  # import_files(files,loan_acquisition)
                except Exception as e:
                    msg=f'Un handled Exception: {e}'
                    self.current_file_state.failed(msg)
                    self.pidManager.checkin('DOWORK','ERROR',msg) 
                    logging.exception(msg)

                # check to see if we have any command
                if self.pidManager.check_commands()=='STOP':
                    self.pidManager.release_claim(db)
                    self.do_post_process_scripts(db,self.foi_list)   
                    sys.exit(0)  

            elif get_work_status == WorkState.NO_MORE_WORK:
                logging.info(f"No More Work Found Exiting Process")
                self.pidManager.checkin('NO_MORE_WORK','DONE') 
            elif get_work_status == WorkState.SLEEP:
                logging.info(f"Woke up From sleep, Checking for more work")  
                self.pidManager.checkin('SLEEPING','SLEEP',"Waiting for Work") 
            else:
                logging.error(f"Unknown Work State Tripped EXITING")
                self.pidManager.checkin('HARD ERROR','ERROR',"Unknown Work State Tripped EXITING")
                sys.exit(1)  
            
        self.do_post_process_scripts(db,self.foi_list)     
    def __str__(self):
          
        self_attributes=[{a:f"{getattr(self,a)}"} for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
       
        return str(self_attributes)
