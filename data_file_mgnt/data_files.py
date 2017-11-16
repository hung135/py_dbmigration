import re
import os
import glob
import zip_utils
import socket
import logging
import hashlib
import subprocess as commands
import time
import datetime as dt
import pandas as pd
import db_logging as lg
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

    def __init__(self, table_name, file_regex, file_delimiter, column_list, schema_name=None, has_header=False, folder_regex=None):
        self.regex = file_regex
        self.folder_regex = folder_regex
        self.table_name = table_name
        self.schema_name = schema_name
        self.column_list = column_list
        self.file_delimiter = file_delimiter
        self.has_header = has_header


def convert_to_sql(instring):
    sqlstring = instring.replace("'", "\'")
    sqlstring = sqlstring.replace('"', '\"')
    return sqlstring


class DataFile:

    def __init__(self, file_path, working_path, regex, db, id_regex=None, file_type=None, compressed_file_type=None):
        self.compressed_file_type = None
        self.source_file_path = None
        self.working_path = None
        self.curr_src_working_file = None
        self.curr_file_sucess = True
        self.curr_pid = None
        self.host = None
        self.source_files = None  # the  files found walking the source directory based on the regex passed in
        self.processed_file_count = None
        self.files = None  # The contents of the zip files for the last file unzipped
        # file_type = None
        self.id_regex = id_regex  # Regex for the id found in the name of the file
        self.update_query = """Update meta_source_files
                           set file_process_state='{0}',
                               process_end_dtm = now()
                           where file_name='{1}' and
                           """
        self.copy_command_sql = "call op_dba.copy_from_host('{0}','{1}','DELIMITER ''|'' CSV')"
        self.copy_command_client_side = """psql -c "\copy {0} FROM '{1}' with (format csv,HEADER, FORCE_NULL ({3}),delimiter '{2}')" """
        self.copy_command_root_path = "/home/dtwork/dw/file_transfers/"
        self.Dblogger = None
        self.create_meta_table(db)
        compiled_regex = re.compile(regex, re.IGNORECASE)
        self.source_file_path = file_path
        self.working_path = working_path
        self.source_files = self.walk_dir(file_path, compiled_regex)
        # self.file_type = file_type
        self.compressed_file_type = compressed_file_type

        logging.debug("Source PATH:{}".format(self.source_file_path))
        logging.debug("Working PATH:".format(self.working_path))
        # logging.debug(self.source_files)
        if len(list(self.source_files)) > 0 :
            self.insert_working_files(db, id_regex)
        else:
            logging.debug("No Files Found while walking source directory: REGEX= {}".format(regex))
        self.curr_pid = os.getpid()
        self.host = socket.gethostbyname(socket.gethostname())
        self.Dblogger = lg.DbLogging(db)

    def append_os_slash(file_path):
        if file_path[-1] != '/':
            file_path += '/'
        if file_path == '':
            file_path = '.'
        return file_path

    def build_sql(self, list, id_regex=None):
        sqlvalue = []
        p = None
        extracted_id = None

        if id_regex is not None:
            p = re.compile(id_regex)
        for i in list:

            extracted_id = None
            file_id = '0'

            if id_regex is not None:
                try:
                    extracted_id = p.findall(i)
                    if len(extracted_id)>0:
                        file_id = extracted_id[0]
                    #print("-----",type(extracted_id),len(extracted_id),extracted_id)
                except Exception as e:
                    logging.warning("No Embedded ID Found in FileName: id_REGEX = {}".format(id_regex))

            sqlvalue.append(
                ("('{0}','{1}','{2}','{3}')").format(i, self.source_file_path, file_id, self.compressed_file_type))
        #logging.debug("Meta DataFiles:{}".format(sqlvalue))
        return sqlvalue

    def insert_working_files(self, db, id_regex=None):
        conflict_clause = "ON CONFLICT (file_path,file_name) DO NOTHING"
        value_list = ',\n'.join(self.build_sql(self.source_files, id_regex))
        if len(value_list)>1:
            db.insert_table('{0}.meta_source_files'.format(db.dbschema),
                        'file_name,file_path,embedded_id,file_type', value_list, conflict_clause)

    def dump_delimited_file(self, db, file_name, delimiter):
        shell_command = """psql -c "copy data_table from '{0}}' WITH DELIMITER AS '{1}' CSV QUOTE AS '"' """
        shell_command.format(file_name, delimiter)

    def reset_meta_table(self, db, option='FAILED'):
        if option == 'ALL':
            db.update(
                """Update {}.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                """.format(db.dbschema))
        if option == 'FAILED':
            logging.debug("RESET META DATA FAILED IMPORTS:")
            db.update(
                """Update {}.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                where upper(file_process_state)='FAILED'
                """.format(db.dbschema))
        db.commit()

    def create_meta_table(self, db):
        creatsql = """CREATE TABLE if not exists {0}.meta_source_files
                (
                  id serial NOT NULL,
                  file_name character varying(256) NOT NULL,
                  file_path character varying(256) NOT NULL,
                  embedded_id character varying(64), -- Data extracted from the file_name
                  file_type character varying(32)  NULL, -- type of file used to determin which code to execute
                  file_process_state character varying(32) DEFAULT 'raw', -- status of the file
                  process_start_dtm timestamp without time zone, -- start time is used to denote something is being processed
                  process_end_dtm timestamp without time zone, -- end time tells us it has completed processing 
                  current_worker_host character varying(64), -- the ip of the host processing the file
                  current_worker_host_pid integer, -- the PID of the process on the host processing the file
                  rows_inserted bigint default 0,
                  data_appended boolean default False,
                  file_size bigint default 0,
                  total_rows bigint default 0, -- Using linux WC to count the number of lines in the file
                  total_files integer default 0, -- if compressed files this is a count of how files inside
                  total_files_processed integer default 0,
                  last_error_msg text, -- last error that occured trying to process this file
                  database_table character varying(64), -- future use to track which database tile this file got inserted into
                  CONSTRAINT pk_meta_file_name PRIMARY KEY (id),
                  CONSTRAINT meta_source_files_file_path_file_name_key UNIQUE (file_path, file_name)
                )

                WITH(
                  OIDS=FALSE
                );
                """
        # logging.debug(creatsql)
        db.create_table(creatsql.format(db.dbschema))

    def walk_dir(self, file_path, regex):
        """Walks a directory structure and returns all files that match the regex pattern
        """
        import os

        if file_path[-1] != '/':
            file_path += '/'
        # files_list = glob.glob1(file_path, "*")
        files_list = []
        # files_list= os.listdir(file_path)
        # print("xxxxxxx",files_list)
        for root, subdirs, files in os.walk(file_path, topdown=True):
            for x in files:
                rel_path = root.replace(file_path, "")
                # print ("-----f-----",root.replace(file_path,""),"---",x)
                if rel_path == "":
                    files_list.append(x)
                else:
                    files_list.append(rel_path + "/" + x)

        match_list = filter(regex.match, files_list)

        if match_list == []:
            match_list = None

        return list(match_list)
    # import one file at a time using client side copy command postgres
    def import_1file_client_side(self, dest, db):
        data_file = self.source_file_path + self.curr_src_working_file
        logging.debug("Into Import:{0}".format(dest.table_name))
        if db is not None:

            logging.debug("Into Import:{0}".format(dest.table_name))
            if re.match(dest.regex, self.curr_src_working_file):
                t = lg.DbLogging(db)

                # data_file = self.source_file_path + self.curr_src_working_file
                error_log_entry = t.ErrorLog(
                    program_unit=sys.argv[0],
                    error_code=None,
                    error_message='Inside Function import_1file_client_side',
                    error_timestamp=None,
                    user_name=db._userid,
                    sql_statement='')
                log_entry = t.LoadStatus(table_name=dest.table_name,
                                         program_unit=sys.argv[0],
                                         program_unit_type_code='python',
                                         file_path=data_file,
                                         records_inserted=0,
                                         success=1,
                                         start_date=dt.datetime.now(),
                                         end_date=dt.datetime.now(),
                                         previous_record_count=0,
                                         current_record_count=0,
                                         records_updated=0,
                                         records_deleted=0,
                                         created_by=db._userid,
                                         created_date=dt.datetime.now())
                copy_string = None
                if len(dest.column_list) >0:
                    print("HAS COLUMNS",dest.column_list)
                    copy_string = "{}({})".format(dest.table_name, ",".join(dest.column_list))
                else:
                    copy_string = dest.schema_name+"."+dest.table_name
                logging.info("Import From file into: {}".format(copy_string))
                command_text = self.copy_command_client_side.format(
                    copy_string, data_file, dest.file_delimiter,",".join(db.get_columns(dest.table_name,dest.schema_name)))

                logging.info("Copy Command STARTED:{0}".format(dest.table_name))

                txt_out = commands.getstatusoutput(command_text)

                logging.info("Command:{0}".format(command_text))
                
                logging.info("OUTPUT:{0} ".format(txt_out))

                if txt_out[0] > 0 and not ('ERROR' in txt_out[1]):
                    self.flag_bad_file(
                        db, self.source_file_path, self.curr_src_working_file, txt_out[1])
                    self.curr_file_sucess = False
                    # flagging each data item we want to log durring an error
                    log_entry.success = 0
                    error_log_entry.error_code = txt_out[0]
                    error_log_entry.error_msg = txt_out[1]
                    error_log_entry.sql_statement = command_text
                    error_log_entry.error_timestamp = dt.datetime.now()
                    t.session.add(error_log_entry)
                    logging.error("Import ERROR Occured: {}".format(txt_out))

                else:
                    self.processed_file_count += 1
                    i = txt_out[1].split()
                    # flagging the logging entry
                    log_entry.end_date = dt.datetime.now()

                    log_entry.records_inserted = i[1]
                    logging.info(
                        "Copy Command Compleated: {0}".format(data_file))
                    t.session.add(log_entry)
                #t.session.commit()
        else:
            logging.debug("Regex Not Match Skipping:{0}".format(dest.table_name))
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
    #leveraging pandas libraries to read csv into a dataframe and let pandas insert into database
    def import_file_pandas(self, dest, db, lowercase=True, limit_rows=None):

        full_file_path = self.source_file_path + self.curr_src_working_file
        status = ''
        if db is not None:
            conn, meta = db.connect_sqlalchemy()

            table_name = dest.table_name
            if lowercase:
                table_name = str.lower(table_name)
            try:
                logging.info("Pandas Reading From File: {0}->{1}".format(self.curr_src_working_file, full_file_path))

                if limit_rows is not None:
                    logging.info("Pandas Read Limit SET: {0}:ROWS".format(limit_rows))
                dataframe = pd.read_csv(self.source_file_path + self.curr_src_working_file,
                                        delimiter=dest.file_delimiter, nrows=limit_rows, quotechar='"')
                dataframe.columns = map(str.lower, dataframe.columns)
                logging.info("Pandas Insert Into DB: {0}->{1}".format(dest.schema_name, dest.table_name))
                dataframe.to_sql(table_name, conn, schema=dest.schema_name, if_exists='append', index=False, chunksize=5000)
                self.Dblogger.insert_LoadStatus(table_name=dest.table_name, program_unit="FileImport",
                                                program_unit_type_code="Pandas",
                                                file_path=full_file_path, success=int(self.curr_file_sucess))
            except Exception as e:
                # status=str(e)

                # logging.error(str(e))
                status = "Error Inserting File"
                cols_tb = db.get_table_columns(str.lower(dest.table_name))
                delta = diff_list(dataframe.columns.tolist(), cols_tb)
                self.curr_file_sucess = False
                self.Dblogger.insert_ErrorLog(error_code="Err", error_message=str(e)[:200],
                                              program_unit="FileImport", user_name=db._userid,
                                              sql_statement=full_file_path)

                cols = list(delta)
                if len(cols) > 1:
                    self.Dblogger.insert_ErrorLog(error_code="Info", error_message="New Columns?",
                                                  program_unit="FileImport", user_name=db._userid,
                                                  sql_statement=str(list(delta)))
                msg="Error Message: {}".format(dest.table_name)
                print(str(e)[:2000])
                self.Dblogger.insert_ErrorLog(error_code="Info", error_message=msg,
                                              program_unit="FileImport", user_name=db._userid,
                                              sql_statement=str(e)[:2000])

                logging.error("ERROR: {0}".format(e))

                # print (db.get_table_columns(str.lower(dest.table_name)))

                # self.Dblogger.insert_Errorlog(table_name=dest.table_name,program_unit="FileImport",
                # program_unit_type_code="Pandas",file_path=full_file_path,success=self.curr_file_sucess,
                # error_timestamp=datetime.now())

            logging.info("Pandas Insert Completed: {0}->{1}".format(self.curr_src_working_file, full_file_path))

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

                    command_text = self.copy_command_client_side.format(
                        dest.schema_name+"."+dest.table_name, full_file_path, dest.file_delimiter)
                    logging.debug(command_text)
                    txt_out = commands.getstatusoutput(command_text)
                    logging.debug(txt_out)
                    if txt_out[0] > 0:
                        self.flag_bad_file(
                            db, self.source_file_path, self.curr_src_working_file, txt_out[1])
                        self.curr_file_sucess = False
                    else:
                        self.processed_file_count += 1
                    logging.info(
                        "Copy Command Compleated: {0}->{1}".format(self.curr_src_working_file, file))
                    self.Dblogger.insert_LoadStatus(table_name=dest.table_name, program_unit="FileImport",
                                                    program_unit_type_code="clientside", file_path=full_file_path,
                                                    success=self.curr_file_sucess)

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
                    relative_file_path = full_file_path.replace(
                        self.copy_command_root_path, "", 1)
                    sql_string = self.copy_command_sql.format(
                        dest.table_name, relative_file_path)
                    logging.debug(sql_string)
                    db.execute(sql_string, False)
                    logging.info(
                        "Copy Command Compleated: {0}->{1}".format(self.curr_src_working_file, file))
                self.processed_file_count += 1

    def cleanup_files(self):
        if self.curr_file_sucess:
            if self.work_file_type == 'DATA':
                logging.debug("Deleting File:{0}".format(self.source_file_path + '/' + self.curr_src_working_file))
                os.remove(self.source_file_path + '/' + self.curr_src_working_file)
                if not os.listdir(self.source_file_path):
                    os.rmdir(self.source_file_path)

    def flag_bad_file(cur, db, file_path, file_name, error_msg):
        logging.warning("Logging bad file:{0} ErrorMsg:{1}".format(file_name, error_msg))
        update_query = """Update {0}.meta_source_files
            set file_process_state='failed',
                process_end_dtm=now(),
                last_error_msg='{3}'
            where file_name='{1}' and
            file_path='{2}'
            """
        error_msg_sql = convert_to_sql(error_msg)

        executable_query = update_query.format(
            db.dbschema, file_name, file_path, error_msg_sql)
        db.update(executable_query)

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

    def insert_each_line(self, orgfile, newfile, string_data):

        with open(newfile, 'w') as outfile:
            with open(orgfile, 'r') as src_file:
                for line in src_file:
                    outfile.write(string_data + line)

    def insert_prepend(self, orgfile, newfile, string_data):
        with open(orgfile, 'r') as f:
            with open(newfile, 'w') as f2:
                f2.write(string_data + f.read())

    def insert_append(self, orgfile, string_data):
        with open(orgfile, 'w') as f:
            f.write(self.source_file_path)

    def insert_into_file(self, file, text_append, option=None):
        newfile = file + 't'
        if option is None or option == 'LINE':
            logging.debug("Appending to Each Line:{0}: Data: {1}".format(file, text_append))
            self.insert_each_line(file, newfile, text_append)
            os.rename(newfile, file)
        elif option == 'HEAD':
            logging.debug("Pre-pend To FileName:{0}:".format(file))
            self.insert_prepend(file, newfile, text_append)
            os.rename(newfile, file)
        elif option == 'APPEND':
            logging.debug("Pre-pend To FileName:{0}:".format(file))
            self.insert(file, text_append)

    def list_current_work(db, host=None):
        logging.info(db.query("""select file_name, current_worker_host from meta_source_files,id
                    where current_worker_host is not null and current_worker_host_pid is not null
                    order by process_start_dtm"""))

    def finish_work(self, db, process_error=None, dbtable=''):
        file_imported = ''
        print(process_error)

        if self.curr_file_sucess:
            file_imported = """,file_process_state='Processed'"""

        sqlstring = """ update {0}.meta_source_files set  process_end_dtm=now()
                ,last_error_msg='{6}',database_table='{7}',total_files_processed={5} {4}
                where current_worker_host='{1}' and current_worker_host_pid={2}
                and file_name='{3}'"""
        db.update(sqlstring.format(db.dbschema, self.host,
                                   self.curr_pid, self.curr_src_working_file,
                                   file_imported, self.processed_file_count, process_error, dbtable))
        db.vacuum()

    def get_work(self, db):
        self.processed_file_count = 0
        self.total_data_file_count = 0
        db.update(("""
                    update {0}.meta_source_files set
                    current_worker_host='{1}', current_worker_host_pid={2}, process_start_dtm=now()
                    where(file_path ||file_name) in (select file_path ||file_name
                        from {0}.meta_source_files where current_worker_host is null order by
                        embedded_id desc ,file_type asc limit 1)
                    """).format(db.dbschema, self.host, self.curr_pid))
        row = db.query(("""select file_name, file_path,embedded_id, file_type,id,total_rows from {0}.meta_source_files where
                         current_worker_host='{1}'
                         and current_worker_host_pid={2}
                        and process_end_dtm is null
                         order by file_type asc,process_start_dtm desc limit 1
                         """).format(db.dbschema, self.host, self.curr_pid))

        if not row:
            self.curr_src_working_file = None
            self.source_file_path = None
            logging.info("No Work Left")

        else:
            self.curr_src_working_file = row[0][0]
            self.source_file_path = row[0][1]
            self.embedded_id = row[0][2]
            self.work_file_type = row[0][3]
            self.total_files = 1
             
            self.file_size = 0
            self.meta_source_file_id = row[0][4]
            self.row_count = row[0][5]
            self.file_size = os.path.getsize(self.source_file_path + '/' + self.curr_src_working_file)

            if self.work_file_type == 'DATA' and self.row_count==0:
                logging.debug("Working DATAFILE:{0}:".format(self.curr_src_working_file))
                self.row_count = self.count_file_lines(self.source_file_path + '/' + self.curr_src_working_file)

                # set_file_size()
                # row_count()

            db.update(("""
                    update {0}.meta_source_files set
                    total_files={3},
                    total_rows={4},
                    file_size={5}
                    where
                        current_worker_host='{1}'
                        and current_worker_host_pid={2}
                        and process_end_dtm is null
                        """).format(db.dbschema, self.host, self.curr_pid, self.total_files, self.row_count, self.file_size))

        return self.curr_src_working_file

    def extract_file(self, db):
        self.files = zip_utils.extract_file(
            self.source_file_path + self.curr_src_working_file, self.working_path +
            self.curr_src_working_file + '/', False, self.work_file_type)
        self.total_files = self.files.total_files

        # We walk the tmp dir and add those data files to list of to do
        new_src_dir = self.working_path + self.curr_src_working_file + '/'
        logging.debug("WALKING EXTRACTED FILES:\src_dir:{0}\nworking_dir:{1}:".format(new_src_dir, self.working_path))
        DataFile(new_src_dir,
                 self.working_path, '.*', db, self.id_regex, 'DATA', 'DATA')
    # Do work will query the meta source table for a record
    # It will stamp that record with this pid and ip address
    # When it is done with the processing of the record it we stamp the process_end_dtm
    # signifying the file has been processed

    def do_work(self, db, datafiles, cleanup=True, limit_rows=None,import_type='Pandas'):
        df = self
        while df.get_work(db) is not None:

            logging.debug("Got New Working File:{0}:".format(self.curr_src_working_file))
            if df.work_file_type == 'DATA':
                year = df.embedded_id[:-2]
                quarter = df.embedded_id[-1:]
                print(self.row_count,"--------")
                # use the line below if we need to stamp the data file w/ a column that has additional data
                # df.insert_into_file(df.source_file_path + df.curr_src_working_file, str(quarter)+str(year)+ '|', 'LINE')
                # one of these won't execute because the file won't match the regex
                process_error = ''
                dbtable = ''
                pattern_found = False
                for x in datafiles:
                    # Data files has to have atleast 2 rows 1 for Header 1 for data.
                    min_row = 0
                    if x.has_header:
                        min_row = 1
                    # print ("Min row count:",min_row,x.has_header)
                    if df.row_count > min_row:
                        if df.match_regex(x.regex, x.folder_regex):
                            if import_type=='Pandas':
                                process_error = df.import_file_pandas(x, db, limit_rows=limit_rows)
                            if import_type=='CopyCommand':
                                process_error = df.import_1file_client_side(x, db)
                            pattern_found = True
                            dbtable = x.table_name

                    else:
                        process_error = "File Has no Data"
                        pattern_found = True
                if not pattern_found:
                    process_error = process_error + "RegEx Pattern Not found for File"
                df.finish_work(db, process_error=process_error, dbtable=dbtable)
            else:
                df.extract_file(db)
                df.finish_work(db)
            if cleanup:
                df.cleanup_files()
            # import_files(files,loan_acquisition)
