import re
import os
import glob
from py_dbmigration import zip_utils
import socket
import logging
import hashlib
import commands
import time
import datetime as dt
import pandas as pd
from py_dbmigration import db_logging as lg
import reflection as msf
from py_dbmigration import db_table
import sys
from subprocess import call


# given 2 data frame this will find all the records do not exist in the right data frame
def dataframe_diff_left(df1, df2, column_name):
    result = pd.merge(df1, df2, on=column_name, how='left', indicator=True)
    return result.query("_merge =='left_only'")


def diff_list(list1, list2):
    delta = []
    # print("---into---diff", type(list1), list1)
    # print("---into---diff2", type(list2), list2)
    l1 = set(list1)
    l2 = set(list2)

    for i in list1:
        # print("innnn---->", i)
        if i not in list2:
            delta.append(i)
    return l1 - l2


class DestinationDB:
    file_type = None
    append_column_name = None
    append_file_id = False
    file_delimiter = ','
    has_header = False
    column_list = None
    regex = None
    file_name_data_regex = None
    file_path = None
    file_list = []
    parent_file_id = 0
    insert_option = None
    encoding ='UTF-8'

    def __init__(self, file_type, file_regex, table_name=None, file_delimiter=None, column_list=None,
                 schema_name=None, has_header=False, folder_regex=None, append_file_id=False,
                 append_column_name='file_id', file_name_data_regex=None, file_path=None,
                 parent_file_id=0, insert_option=None,encoding='UTF-8'):
        self.regex = file_regex
        self.folder_regex = folder_regex
        self.table_name = table_name
        self.schema_name = schema_name
        self.column_list = column_list
        self.file_delimiter = file_delimiter
        self.has_header = has_header
        self.append_file_id = append_file_id
        self.append_column_name = append_column_name
        self.file_type = file_type
        self.file_name_data_regex = file_name_data_regex
        self.file_path = file_path
        self.parent_file_id = parent_file_id
        self.insert_option = insert_option
        self.encoding=encoding



def get_mapped_table(file_name, dest):
    for i in dest:
        if re.match(i.regex, file_name, re.IGNORECASE):
            return i

    return None


def convert_to_sql(instring):
    sqlstring = instring.replace("'", "\'")
    sqlstring = sqlstring.replace('"', '\"')
    return sqlstring


class DataFile:
    working_path = None
    db = None
    file_pattern_list = None
    current_file_pattern = None
    update_query = """Update meta_source_files
                       set file_process_state='{0}',
                           process_end_dtm = now()
                       where file_name='{1}' and
                       """
    copy_command_sql = "call op_dba.copy_from_host('{0}','{1}','DELIMITER ''|'' CSV')"
    copy_command_client_side = """psql -c "\copy {0} FROM '{1}' with (format csv,{4} FORCE_NULL ({3}),delimiter '{2}', ENCODING '{5}')" """
    meta_source_file_id = 0

    def __init__(self, working_path, db, file_pattern_list, parent_file_id=0):

        self.db = db
        self.working_path = working_path
        self.file_pattern_list = file_pattern_list
        # self.compressed_file_type = None
        #self.file_pattern_list = file_pattern_list
        self.source_file_path = None

        self.curr_src_working_file = None
        self.curr_file_success = True
        self.curr_pid = None
        self.host = None
        self.source_files = None  # the  files found walking the source directory based on the regex passed in
        self.processed_file_count = None
        self.files = None  # The contents of the zip files for the last file unzipped
        file_type = None

        # self.copy_command_root_path = "/home/dtwork/dw/file_transfers/"

        # self.create_meta_table(db)

        # compiled_regex = self.validate_regex(regex)
        # self.source_file_path = file_path
        # self.working_path = working_path
        # take each pattern and walks the directory
        for DestinationDB in file_pattern_list:
            #print(DestinationDB.file_path,"about to wal -----")
            self.DestinationDB = self.walk_dir(DestinationDB, level=1)
            # print(type(self.source_files), "----list----")
            self.DestinationDB.parent_file_id = self.meta_source_file_id
            try:
                logging.debug(self.DestinationDB.files_list)
            except:
                pass
            if len(list(self.DestinationDB.file_list)) > 0:
                #print(self.DestinationDB.file_list, "----Match-----",self.DestinationDB.regex)
                self.insert_working_files(db, self.DestinationDB)
            else:
                #print(self.DestinationDB.files_list,"<--------->",DestinationDB.files_list)

                logging.debug(
                    "No Files Found while walking source directory: file_type= {}".format(self.DestinationDB.file_type))
                logging.debug(
                    "No Files Found while walking source directory: file_path= {}".format(self.DestinationDB.file_path))
                logging.debug("No Files Found while walking source directory: REGEX= {}".format(self.DestinationDB.regex))
        self.curr_pid = os.getpid()
        self.host = socket.gethostname()
        #self.host = socket._LOCALHOST  # self.Dblogger = lg.DbLogging(db)
        # self.Dblogger = lg.db_logging.DbLogging(db)

    # compiles a given regext and will returned a compiled regex
    # will logg and error and returnx None if regext can not be compiled
    def validate_regex(self, regex):
        compiled_regex = None

        try:
            compiled_regex = re.compile(regex, re.IGNORECASE)
        except Exception as e:
            logging.error("Invalid Regex: {}".format(regex))
            raise
        else:
            pass
        finally:
            pass
        return compiled_regex

    def append_os_slash(file_path):
        if file_path[-1] != '/':
            file_path += '/'
        if file_path == '':
            file_path = '.'
        return file_path

    # id_regex is used to extract some meaning data from the name of the file like a year or month or date
    # that id gets stored with the meta data about the file to later use


    def insert_working_files(self, db, DestinationDB):
        t = db_table.RecordKeeper(db)
        id_regex = DestinationDB.file_name_data_regex
        #print("------insertworkingfile")

        for walked_filed_name in DestinationDB.file_list:
            p = None
            extracted_id = None
            #if get_mapped_table(walked_filed_name, self.file_pattern_list):
            if id_regex is not None:
                p = re.compile(id_regex)
            extracted_id = None
            file_id = '0'
            if id_regex is not None:
                try:
                    extracted_id = p.findall(walked_filed_name)
                    if len(extracted_id) > 0:
                        file_id = extracted_id[
                            0]  # print("-----",type(extracted_id),len(extracted_id),extracted_id)
                except Exception as e:
                    logging.warning("No Embedded ID Found in FileName: id_REGEX = {}".format(id_regex))

            row = db_table.MetaSourceFiles(file_path=DestinationDB.file_path, file_name=walked_filed_name,
                                           file_name_data=file_id, file_type=DestinationDB.file_type,
                                           parent_file_id=DestinationDB.parent_file_id)
            t.add_record(row, commit=True)

    def dump_delimited_file(self, db, file_name, delimiter):
        shell_command = """psql -c "copy data_table from '{0}}' WITH DELIMITER AS '{1}' CSV QUOTE AS '"' """
        shell_command.format(file_name, delimiter)

    def reset_meta_table(self, db, option='FAILED'):
        if option == 'ALL':
            db.update("""Update logging.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                """)
        if option == 'FAILED':
            logging.debug("RESET META DATA FAILED IMPORTS:")
            db.update("""Update logging.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                where upper(file_process_state)='FAILED'
                """)
        if option == 'RAW':
            logging.debug("RESET META DATA RAW IMPORTS:")
            db.update("""Update logging.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                where upper(file_process_state)='RAW'
 
                and file_type in ('CSV','DATA')
                """)
        if option == 'DATA':
            logging.debug("RESET META DATA   IMPORTS:")
            db.update("""Update logging.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                where  file_type in ('CSV','DATA')
                """)
        db.commit()

    def walk_dir(self, DestinationDB, level=0):
        """Walks a directory structure and returns all files that match the regex pattern
        """
        import os
        file_path = DestinationDB.file_path
        regex = re.compile(DestinationDB.regex)

        if file_path[-1] != '/':
            file_path += '/'
        # files_list = glob.glob1(file_path, "*")
        files_list = []
        # files_list= os.listdir(file_path)
        # print("xxxxxxx",files_list)
        ii = 0
        for root, subdirs, files in os.walk(file_path, topdown=True):
            if ii < level:
                for x in files:
                    rel_path = root.replace(file_path, "")
                    # print ("-----f-----",root.replace(file_path,""),"---",x)
                    # logging.debug(f"Walking Directory:{subdirs}:{x}")
                    if rel_path == "":
                        files_list.append(x)
                    else:
                        files_list.append(rel_path + "/" + x)
            ii += 1
        # logging.debug("Done Walking Directory:")
        match_list = list(filter(regex.match, files_list))

        logging.debug("Done Walking Directory:{}".format(list(match_list)))

        DestinationDB.file_list = match_list
        #print(match_list,DestinationDB.file_list,"------")
        return DestinationDB

    # import one file at a time using client side copy command postgres
    def import_1file_client_side(self, dest, db):
        error_msg=None
        data_file = dest.full_file_name
        self.rows_inserted=0
        logging.debug("Into Import CopyCommand: {0}".format(dest.schema_name + "." + dest.table_name))
        if db is not None:

            # logging.debug("Into Import:{0}".format(dest.table_name))
            #if re.match(dest.regex, dest.full_file_name):
            t = lg.db_logging.DbLogging(db)

            # data_file = self.source_file_path + self.curr_src_working_file
            error_log_entry = t.ErrorLog(program_unit=sys.argv[0], error_code=None,
                                         error_message='Inside Function import_1file_client_side',
                                         error_timestamp=None, user_name=db._userid, sql_statement='')

            log_entry = t.LoadStatus(table_name=dest.table_name, program_unit=sys.argv[0],
                                     program_unit_type_code='python', file_path=data_file, records_inserted=0,
                                     success=1, start_date=dt.datetime.now(), end_date=dt.datetime.now(),
                                     previous_record_count=0, current_record_count=0, records_updated=0,
                                     records_deleted=0, created_by=db._userid, created_date=dt.datetime.now())
            copy_string = None
            if dest.column_list is not None:
                #print("HAS COLUMNS", dest.column_list)

                copy_string = "{}({})".format(dest.schema_name + "." + dest.table_name, ",".join(dest.column_list))
                #    dest.column_list.replace(' ', '').replace('\n', '').strip(',')))
            else:
                copy_string = dest.schema_name + "." + dest.table_name
            logging.info("Import From file into: {}".format(copy_string))

            cols = db.get_columns(dest.table_name, dest.schema_name)

            header = ''
            if dest.has_header:
                header = 'HEADER,'

            command_text = self.copy_command_client_side.format(copy_string, data_file, dest.file_delimiter,
                                                                ",".join(cols), header,dest.encoding)

            logging.info("Copy Command STARTED:{0}".format(dest.table_name))


            txt_out = commands.getstatusoutput(command_text)
            #txt_out = commands.getoutput(command_text)    
            #print("--------",command_text,"---------")
            #print("--------",txt_out,"---------")
            if ('ERROR' in txt_out[1]):
                logging.info("ERROR running Copy Copy Command")
                logging.error(txt_out)

            logging.info("Command:{0}".format(command_text))

            logging.info("OUTPUT:{0} ".format(txt_out))

            # if txt_out[0] > 0 and not ('ERROR' in txt_out[1]):
            if int(txt_out[0]) > 0:
                print("ERROR---",txt_out[1])
                #self.flag_bad_file(db, self.source_file_path, self.curr_src_working_file, txt_out[1])
                self.curr_file_success = False
                # flagging each data item we want to log durring an error
                log_entry.success = 0
                error_log_entry.error_code = txt_out[0]
                error_log_entry.error_message = txt_out[1]
                error_log_entry.sql_statement = command_text
                error_log_entry.error_timestamp = dt.datetime.now()
                t.session.add(error_log_entry)
                logging.error("Copy Command ERROR Occured: {}".format(txt_out))
                error_msg=txt_out[1]
            else:
                self.processed_file_count += 1
                i = txt_out[1].split()
                # flagging the logging entry
                log_entry.end_date = dt.datetime.now()

                self.rows_inserted= i[1]
                log_entry.records_inserted = i[1]
                logging.info("Copy Command Completed: {0}".format(data_file))
                t.session.add(log_entry)
            try:
                t.session.commit()
            except Exception as e:
                print("-----------------------------------")
                logging.error(e)
                error_msg=e
                logging.debug("Error Occured Logging, Exiting Application")
                #sys.exit()
            else:
                pass
            finally:
                t.session.close()

        else:
            logging.debug("Regex Not Match Skipping:{0}".format(dest.table_name))
        return error_msg

    """
    Test the currentworking file againsts the regex passted in
    """

    def match_regex(self, regex, folder_regex=None):
        t = False

        if re.match(regex, self.curr_src_working_file, re.IGNORECASE):
            if folder_regex is not None:
                if re.match(folder_regex, self.source_file_path, re.IGNORECASE):
                    t = True

            else:
                t = True
        return t

    # leveraging pandas libraries to read csv into a dataframe and let pandas insert into database
    def import_file_pandas(self, dest, db, lowercase=True, limit_rows=None, chunk_size=10000):
        self.Dblogger = lg.db_logging.DbLogging(db)
        full_file_path = None
        self.rows_inserted=0
        status = ''
        header = 'infer'
        names = None
        if db is not None:
            conn, meta = db.connect_sqlalchemy()

            table_name = dest.table_name
            counter = 0
            if lowercase:
                table_name = str.lower(table_name)
            try:
                logging.debug("Pandas Reading From File: {0}".format(dest.full_file_name))

                if limit_rows is not None:
                    logging.debug("Pandas Read Limit SET: {0}:ROWS".format(limit_rows))
                if dest.has_header:
                    header = 'infer'
                    names = None
                else:
                    header = 0
                    names = ','.join(dest.column_list)

                for dataframe in pd.read_csv(dest.full_file_name,
                                             delimiter=dest.file_delimiter, nrows=limit_rows, quotechar='"',
                                             chunksize=chunk_size, header=header):
                    # print(dataframe)
                    if not dest.has_header:
                         
                        dataframe.columns = map( str, dest.column_list)
                        #dataframe.columns = map(str.lower, dataframe.columns)
                        #print("----- printing3",dest.column_list, dataframe.columns)
                    logging.debug(
                        "Pandas Insert Into DB: {0}->{1}-->Records:{2}".format(dest.schema_name, dest.table_name,
                                                                               counter * chunk_size))

                    dataframe.to_sql(table_name, conn, schema=dest.schema_name, if_exists='append', index=False,

                                     index_label=names)
                    counter = counter + 1
                self.rows_inserted= counter*chunk_size
                self.Dblogger.insert_LoadStatus(table_name=dest.table_name, program_unit="FileImport",
                                                program_unit_type_code="Pandas", file_path=dest.full_file_name,
                                                success=int(self.curr_file_success))
            except Exception as e:
                # status=str(e)

                # logging.error(str(e))
                status = "Error Inserting File"
                delta=''
                try:
                    cols_tb = db.get_table_columns(str.lower(dest.table_name))
                    delta = diff_list(dataframe.columns.tolist(), cols_tb)
                except:
                    pass
                #delta = diff_list(dataframe.columns.tolist(), cols_tb)
                self.curr_file_success = False
                self.Dblogger.insert_ErrorLog(error_code="Err", error_message=str(e)[:200], program_unit="FileImport",
                                              user_name=db._userid, sql_statement=full_file_path)

                cols = list(delta)

                if len(cols) > 1:
                    self.Dblogger.insert_ErrorLog(error_code="Info", error_message="New Columns?",
                                                  program_unit="FileImport", user_name=db._userid,
                                                  sql_statement=str(list(delta)))
                msg = "Error Message: {}".format(dest.table_name)
                print(str(e)[:2000])
                self.Dblogger.insert_ErrorLog(error_code="Info", error_message=msg, program_unit="FileImport",
                                              user_name=db._userid, sql_statement=str(e)[:2000])

                logging.error("ERROR: {0}".format(e))

                # print (db.get_table_columns(str.lower(dest.table_name)))

                # self.Dblogger.insert_Errorlog(table_name=dest.table_name,program_unit="FileImport",  # program_unit_type_code="Pandas",file_path=full_file_path,success=self.curr_file_success,  # error_timestamp=datetime.now())

            logging.debug("Pandas Insert Completed: {0}->{1}".format(self.curr_src_working_file, full_file_path))

        return status

    def import_files_client_side(self, dest, db):
        """ this won't be used right now, using copy command instead

        for file in files.list:
            if re.match(dest.regex, file):
                print 'Match', files.path+'/'+file
                import_csv(dest.schema_name, dest.table_name,
                           files.path+'/'+file, dest.column_list)
        """
        # compiled_regex = re.compile(error_regex)
        if db is not None:
            for file in self.files.list:
                if re.match(dest.regex, file, re.IGNORECASE):
                    full_file_path = self.files.path + file

                    command_text = self.copy_command_client_side.format(dest.schema_name + "." + dest.table_name,
                                                                        full_file_path, dest.file_delimiter)
                    logging.debug(command_text)
                    txt_out = commands.getstatusoutput(command_text)
                    logging.debug(txt_out)
                    if txt_out[0] > 0:
                        self.flag_bad_file(db, self.source_file_path, self.curr_src_working_file, txt_out[1])
                        self.curr_file_success = False
                    else:
                        self.processed_file_count += 1
                    logging.info("Copy Command Compleated: {0}->{1}".format(self.curr_src_working_file, file))
                    self.Dblogger.insert_LoadStatus(table_name=dest.table_name, program_unit="FileImport",
                                                    program_unit_type_code="clientside", file_path=full_file_path,
                                                    success=self.curr_file_success)

    def import_files_copy_cmd(self, dest, db):
        """ this won't be used right now, using copy command instead

        for file in files.list:
            if re.match(dest.regex, file):
                print 'Match', files.path+'/'+file
                import_csv(dest.schema_name, dest.table_name,
                           files.path+'/'+file, dest.column_list)
        """

        if db is not None:
            for file in self.files.list:
                if re.match(dest.regex, file, re.IGNORECASE):
                    full_file_path = self.files.path + '/' + file
                    relative_file_path = full_file_path.replace(self.copy_command_root_path, "", 1)
                    sql_string = self.copy_command_sql.format(dest.table_name, relative_file_path)
                    logging.debug(sql_string)
                    db.execute(sql_string, False)
                    logging.info("Copy Command Compleated: {0}->{1}".format(self.curr_src_working_file, file))
                self.processed_file_count += 1

    def cleanup_files(self):
        if self.curr_file_success:
            if self.work_file_type == 'DATA':
                logging.debug("Deleting File:{0}".format(self.source_file_path + '/' + self.curr_src_working_file))
                os.remove(self.source_file_path + '/' + self.curr_src_working_file)
                if not os.listdir(self.source_file_path):
                    os.rmdir(self.source_file_path)

    def md5_for_file(path, block_size=256 * 128, hr=False):
        '''
        Block size directly depends on the block size of your filesystem
        to avoid performances issues
        Here I have blocks of 4096 octets (Default NTFS)
        '''
        md5 = hashlib.md5()
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                md5.update(chunk)
        if hr:
            return md5.hexdigest()
        return md5.digest()

    def count_csv(self, full_file_path):
        import csv
        import datetime
        starttime = datetime.datetime.now()
        # print("starttime", starttime)
        logging.debug("Counting File: {}".format(datetime.datetime.now()))
        chunksize = 10 ** 5
        i = 0
        for chunk in pd.read_csv(full_file_path, chunksize=chunksize):
            i += 1  # print(i,chunksize,i*chunksize,datetime.datetime.now())
            #if i > 10:
                #break  # tempory for testing
        if i > 0:
            count_size = len(chunk) + (
                    i - 1) * chunksize  # print(chunk.columns,len(chunk)+(i-1)*chunksize,starttime,datetime.datetime.now())
        else:
            count_size = 0
        logging.debug("File Row Count:{0}".format(count_size))
        return count_size

    def count_file_lines(self, file):
        # this is 10-100x slower than using WC command
        # command_output = commands.getstatusoutput("wc -l '{}'".format(file))
        # logging.debug("FileName:{0} RowCount:{1}".format(file, command_output))
        logging.debug("Counting File:")
        num_lines = sum(1 for line in open(file))
        logging.debug("File Row Count:{0}".format(num_lines))
        return num_lines

    def count_file_lines_wc(self, file):
        command_output = commands.getstatusoutput("wc -l '{}'".format(file))
        logging.debug("FileName:{0} RowCount:{1}".format(file, command_output))

        return command_output

    def insert_each_line(self, orgfile, newfile, string_data, header_name,has_header):
        import os
        import errno

        if not os.path.exists(os.path.dirname(newfile)):
            try:
                os.makedirs(os.path.dirname(newfile))
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise

        with open(newfile, 'w') as outfile:
            with open(orgfile, 'r') as src_file:
                ii = 0
                for line in src_file:
                    if ii == 0 and header_name is not None and has_header:
                        outfile.write(header_name + line)
                    else:
                        outfile.write(string_data + line)
                    ii += 1

    def insert_prepend(self, orgfile, newfile, string_data):
        with open(orgfile, 'r') as f:
            with open(newfile, 'w') as f2:
                f2.write(string_data + f.read())

    def insert_append(self, orgfile, string_data):
        with open(orgfile, 'w') as f:
            f.write(self.source_file_path)

    def insert_into_file(self, file, newfile, text_append, option=None, header_name=None,has_header=True):

        if option is None or option == 'LINE':
            #logging.debug("Appending to Each Line:{0}: Data: {1}".format(file, header_name, text_append,has_header,"<---Has Header"))
            #print(f"------{newfile}-xxxx")
            logging.debug("Appending File ID to File:{}".format(newfile))
            self.insert_each_line(file, newfile, text_append, header_name,has_header)

            # os.rename(newfile, file)
        # not used yet
        elif option == 'HEAD':
            logging.debug("Pre-pend To FileName:{0}:".format(file))
            self.insert_prepend(file, newfile, text_append)
            # os.rename(newfile, file)
        # no usecase yet
        elif option == 'APPEND':
            logging.debug("Pre-pend To FileName:{0}:".format(file))
            self.insert(file, text_append)
        return newfile

    def list_current_work(db, host=None):
        logging.info(db.query("""select file_name, current_worker_host from meta_source_files,id
                    where current_worker_host is not null and current_worker_host_pid is not null
                    order by process_start_dtm"""))

    def finish_work(self, db, process_error=None, dest=None, vacuum=True):
        t = db_table.MetaSourceFiles()
        t = db_table.RecordKeeper(db)
        row = t.get_record(db_table.MetaSourceFiles.id == self.meta_source_file_id)
        row.process_end_dtm = dt.datetime.now()

        if self.curr_file_success:
            row.file_process_state = 'Processed'
            if dest is not None:
                row.database_table=dest.schema_name+'.'+dest.table_name
            row.rows_inserted=self.rows_inserted
        if process_error is not None and process_error !='':
            row.last_error_msg = process_error
            row.file_process_state = 'Failed'
            row.rows_inserted=0


        if vacuum and dest is not None and process_error is None:
            db.vacuum(dest.schema_name, dest.table_name)
        t.commit()
        t.session.close

    def pre_process_file(self, db, dest):
        pass

    def get_work(self, db, dest):
        self.processed_file_count = 0
        self.total_data_file_count = 0

        t = db_table.RecordKeeper(db)

        # to ensure we lock 1 row to avoid race conditions
        t.engine.execute(("""
                    update {0}.meta_source_files set
                    current_worker_host='{1}', current_worker_host_pid={2}, process_start_dtm=now()
                    where(file_path ||file_name) in (select file_path ||file_name
                        from {0}.meta_source_files where current_worker_host is null order by
                        file_size asc, file_name_data desc ,file_type asc limit 1)
                    """).format(db_table.MetaSourceFiles.DbSchema, self.host, self.curr_pid))

        row = t.get_record(db_table.MetaSourceFiles.current_worker_host == self.host,
                           db_table.MetaSourceFiles.current_worker_host_pid == self.curr_pid,
                           db_table.MetaSourceFiles.process_end_dtm == None)

        if row is None:
            logging.info("No Work Left")
            return None

        else:
            self.curr_src_working_file = row.file_name
            self.source_file_path = row.file_path
            self.embedded_id = row.file_name_data
            self.work_file_type = row.file_type
            self.total_files = row.total_files
            self.file_size = row.file_size
            self.meta_source_file_id = row.id
            self.row_count = row.total_rows
            #print(f"{row.total_rows}xxxxxx")

            try:

                self.file_size = os.path.getsize(self.source_file_path + '/' + self.curr_src_working_file)

                row.file_size = self.file_size
                t.commit()
                if self.work_file_type in( 'DATA','CSV') and self.row_count == 0:
                    #logging.debug("Working DATAFILE:{0}:".format(self.curr_src_working_file))
                    row.total_files = 1
                    logging.debug("Counting File : {}".format(self.source_file_path + '/' + self.curr_src_working_file))
                    self.row_count = self.count_csv(self.source_file_path + '/' + self.curr_src_working_file)
                    row.total_rows = self.row_count
                    logging.debug("Counting File Result:{0}:".format(self.row_count))




            except Exception as e:
                row.last_error_msg = str(e)

                logging.debug("Flagging Bad File: {}".format(self.curr_src_working_file))
                logging.error(e)
                self.curr_file_success = False


            else:
                pass
            finally:

                t.commit()
                t.session.close()

        # m=msf.MetaSourceFiles(db)

        # for d in m.get_row_meta_source_files():

        #     # print(dir(d),type(d),"xxxxxxxx")
        #     print(d.id,d.file_type)
        #     d.file_type='ZZZ'
        #     m.session.add(d)
        #     m.session.commit()

        return self.curr_src_working_file

    def extract_file(self, db, full_file_path, full_writable_path):
        self.files = zip_utils.extract_file(full_file_path,
                                            full_writable_path, False, self.work_file_type)
        t = db_table.RecordKeeper(db)
        row = t.get_record(db_table.MetaSourceFiles.id == self.meta_source_file_id)
        self.total_files = len(self.files)
        row = self.total_files
        t.commit()

        # We walk the tmp dir and add those data files to list of to do
        new_src_dir = full_writable_path
        logging.debug("WALKING EXTRACTED FILES:\src_dir:{0}\nworking_dir:{1}:".format(new_src_dir, self.working_path))
        #print("-----",full_writable_path)
        file_table_map = [DestinationDB('DATA', r'.*', file_path=full_writable_path, file_name_data_regex=None,parent_file_id=self.meta_source_file_id)]

        DataFile(new_src_dir, db, file_table_map, parent_file_id=self.meta_source_file_id)

    # Do work will query the meta source table for a record
    # It will stamp that record with this pid and ip address
    # When it is done with the processing of the record it we stamp the process_end_dtm
    # signifying the file has been processed

    def do_work(self, db, datafiles, cleanup=True, limit_rows=None, import_type='Pandas', vacuum=True, chunksize=10000):
        df = self

        while self.get_work(db, datafiles) is not None:
            logging.debug("Got New Working File:{0}:".format(self.curr_src_working_file))
            full_file_name = self.source_file_path + '/' + self.curr_src_working_file
            if self.work_file_type in ('DATA', 'CSV'):
                # check the current file against our list of regex to see if it matches any table mapping
                x = get_mapped_table(self.curr_src_working_file, datafiles)
                if x is not None and x.insert_option == 'Truncate':
                    db.truncate_table(x.schema_name, x.table_name)
                    print("Truncating Data:{}.{}".format(x.schema_name,x.table_name))
                else:
                    print("Appending  Data:{}.{}".format(x.schema_name,x.table_name))
                logging.debug("DATA-File:{}".format(self.curr_src_working_file))
                if x is None:
                    break
                # use the line below if we need to stamp the data file w/ a column that has additional data
                x.full_file_name = full_file_name
                if x.append_file_id:
                    full_file_name = self.source_file_path +"/"+ self.curr_src_working_file
                    print(self.working_path, "/appended/", self.curr_src_working_file)
                    full_file_name_new = self.working_path + "appended/" + self.curr_src_working_file
                    #print(f"-----{full_file_name}----")
                    self.insert_into_file(full_file_name, full_file_name_new,
                                          str(self.meta_source_file_id) + x.file_delimiter, 'LINE'
                                          , x.append_column_name + x.file_delimiter,x.has_header)
                    x.working_path = self.working_path + "appended/"

                    x.full_file_name = full_file_name_new

                process_error = None
                dbtable = None
                pattern_found = False

                if x is not None:
                    logging.debug("Matched A Table Mapping:{}".format(self.curr_src_working_file))
                    # Data files has to have atleast 2 rows 1 for Header 1 for data.
                    min_row = 0
                    if x.has_header:
                        min_row = 1
                    else:
                        try:
                            x.column_list = db.get_columns(x.table_name, x.schema_name)

                        except:
                            print("No table found: Skipping Column_list")
                        #x.column_list = db.get_columns(x.table_name, x.schema_name)
                    #print(""df.row_count, min_row)
                    logging.debug("File Row Count:{}".format(df.row_count))
                    if df.row_count > min_row:
                        if df.match_regex(x.regex, x.folder_regex):
                            if import_type == 'Pandas':
                                logging.debug("Into Pandas")
                                process_error = self.import_file_pandas(x, db, limit_rows=limit_rows,
                                                                        chunk_size=chunksize)

                            if import_type == 'CopyCommand':
                                logging.info("Import Via Copy Command:{}".format(x.full_file_name))
                                process_error = self.import_1file_client_side(x, db)
                                logging.info("Copy Command result:{}".format(process_error))
                            pattern_found = True
                    if not self.curr_file_success:
                        vacuum = False
                    print("--------Process Error: ",process_error)
                    if process_error=='':
                        process_error=None
                    df.finish_work(db, process_error=process_error, dest=x, vacuum=vacuum)

                else:
                    process_error = "RegEx Pattern Not found for File"
                    df.finish_work(db, process_error=process_error, dest=x, vacuum=vacuum)

            else:
                full_file_name = self.source_file_path + self.curr_src_working_file
                #print("Extracting file, FileType:{self.work_file_type}{full_file_name}/{self.working_path}")
                self.extract_file(db, full_file_name, self.working_path + '/' + self.curr_src_working_file)
                self.finish_work(db, vacuum=vacuum)
            if cleanup:
                self.cleanup_files()  # import_files(files,loan_acquisition)
