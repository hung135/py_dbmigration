import re
import os

import zip_utils
import socket

import commands

import datetime
import pandas as pd
import db_logging
import db_table.db_table_def
import db_table.db_table_func
import db_utils
import sys

import migrate_utils.static_func
import logging as log

logging = log.getLogger()
logging.setLevel(log.DEBUG)


# given 2 data frame this will find all the records do not exist in the
# right data frame
def dataframe_diff_left(df1, df2, column_name):
    result = pd.merge(df1, df2, on=column_name, how='left', indicator=True)
    return result.query("_merge =='left_only'")


def diff_list(list1, list2):
    delta = []
    l1 = set(list1)
    l2 = set(list2)

    for i in list1:
        if i not in list2:
            delta.append(i)
    return l1 - l2


# Struct used to group parameters to define files of interests
class FilesOfInterest:

    # 2 scerios...given a path and a file pattern we walk the dir
    # gven table_name and a file regex we use it to map files from the meta source to a table
    def __init__(self, file_type, file_regex, table_name=None, file_delimiter=None, column_list=None, schema_name=None,
                 has_header=False, folder_regex=None, append_file_id=False, append_column_name='file_id',
                 file_name_data_regex=None, file_path=None, parent_file_id=0, insert_option=None, encoding='UTF-8',
                 append_crc=False, limit_rows=None, start_row=0):
        # avoid trying to put any logic here
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
        self.append_crc = append_crc

        if file_path is not None:
            self.file_path = file_path = os.path.abspath(file_path)
        else:
            self.file_path = None
        self.parent_file_id = parent_file_id
        self.insert_option = insert_option
        self.encoding = encoding
        self.total_files = 0

        self.encoding = encoding
        self.current_working_abs_file_name = None
        self.limit_rows = limit_rows
        self.header_list_returned = None
        self.header_added = None
        self.start_row = start_row

    #def __str__(self):



def get_mapped_table(file_name, foi_list):
    for i in foi_list:
        if i.table_name is not None:
            assert isinstance(i, FilesOfInterest)
            # print("***FOI.regex:",i.regex,i.table_name,file_name)
            if re.match(i.regex, file_name, re.IGNORECASE):
                logging.info("\t\tFile->Table mapping found: {}".format(i.table_name))
                return i
    return None


def convert_to_sql(instring):
    sqlstring = instring.replace("'", "\'")
    sqlstring = sqlstring.replace('"', '\"')
    return sqlstring


# Main class that will handle the lifecycle of a data file:
#       acquisition from the source directory
#       extracted, processed, tagged w/ file_id, checksum every row
#       imported into target database table
#       logs any errorr
#           Pandas will import in chunks
#           Client side CopyCommand All or Nothing

class DataFile:
    FILE_TYPE_ZIP = 'ZIP'
    FILE_TYPE_GZ = 'GZ'
    FILE_TYPE_TAR = 'TAR'
    FILE_TYPE_CSV = 'CSV'
    FILE_TYPE_DATA = 'DATA'
    IMPORT_VIA_PANDAS = 'IMPORT_VIA_PANDAS'
    IMPORT_VIA_CLIENT_CLI = 'IMPORT_VIA_CLIENT_CLI'
    IMPORT_VIA_CUSTOM_FUNC = 'IMPORT_VIA_CUSTOM_FUNC'
    working_path = None
    db = None
    file_pattern_list = None
    current_file_pattern = None
    update_query = """Update meta_source_files
                       set file_process_state='{0}',
                           process_end_dtm = now()
                       WHERE  file_name='{1}' and
                       """
    meta_source_file_id = 0

    def __init__(self, working_path, db, foi_list, parent_file_id=0, compressed_file_type=None):
        assert isinstance(foi_list[0], FilesOfInterest)
        self.parent_file_id = parent_file_id
        self.db = db
        self.working_path = os.path.abspath(working_path)
        self.file_pattern_list = foi_list
        self.compressed_file_type = compressed_file_type
        # self.file_pattern_list = file_pattern_list
        self.source_file_path = None

        self.curr_src_working_file = None
        self.curr_pid = None
        self.host = None
        # the  files found walking the source directory based on the regex
        # passed in
        self.source_files = None
        self.processed_file_count = None
        self.files = None  # The contents of the zip files for the last file unzipped
        self.curr_pid = os.getpid()
        self.host = socket.gethostname()
        self.embedded_id = 0
        self.work_file_type = 0
        self.total_files = 0
        self.curr_file_success = False
        self.processed_file_count = 0
        self.total_data_file_count = 0
        self.foi_list = foi_list
        self.rows_inserted = 0  # initialzed in constructor
        # self.table_file_regex = \
        # self.put_foi_to_db(db, foi_list)

        # take each pattern and walks the directory
        for files_of_interest in self.foi_list:
            if files_of_interest.file_path is not None:
                assert isinstance(files_of_interest, FilesOfInterest)

                self.FilesOfInterest = self.walk_dir(files_of_interest, level=5)

                self.FilesOfInterest.parent_file_id = self.meta_source_file_id

                logging.debug(self.FilesOfInterest.file_list)

                if not 0 >= len(list(self.FilesOfInterest.file_list)):
                    # print(self.FilesOfInterest.file_list, "----Match-----",self.FilesOfInterest.regex)
                    self.insert_working_files(db, self.FilesOfInterest, self.parent_file_id)
                else:
                    # print(self.FilesOfInterest.files_list,"<--------->",FilesOfInterest.files_list)

                    logging.debug(
                        "No Files Found while walking source directory: file_type= {} \n file_path= '{}' \n REGEX= '{}'".format(
                            self.FilesOfInterest.file_type, self.FilesOfInterest.file_path, self.FilesOfInterest.regex))

    # function that will append the file id passed in to every row in a data file.
    # also adding fucntion to generate a checksum of that row for later use

    def insert_into_file(self,foi,file_id,db=None):
        assert isinstance(foi, FilesOfInterest)
        # logging.debug("Appending to Each Line:{0}: Data: {1}".format(file, header_name, text_append,has_header,"<---Has Header"))
        header_added = False
        # logging.debug("Appending File ID to File:{}".format(newfile))

        newfile = os.path.join(self.working_path, "appended/", self.curr_src_working_file)
        header_added, header_list_returned = migrate_utils.static_func.insert_each_line(
            foi.current_working_abs_file_name,
            newfile,
            str(file_id),
            foi.file_delimiter, foi.has_header,foi.append_file_id,
            foi.append_crc, db,foi.table_name,
            foi.limit_rows, foi.start_row
            )
        # return fullpath to new file
        return newfile, header_added, header_list_returned

    @migrate_utils.static_func.timer
    def put_foi_to_db(self, db, foi_list):
        tfr = []
        assert isinstance(db, db_utils.dbconn.Connection)
        assert isinstance(foi_list, list)
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        for foi in foi_list:
            if foi.regex is not None and foi.table_name is not None:
                row = db_table.db_table_def.TableFilesRegex(
                    regex=foi.regex,
                    db_schema=db.dbschema,
                    table_name=foi.table_name,
                    last_update_time=datetime.datetime.now(),
                    active=True
                )

                t.add_record(row, commit=True)
        t.session.commit
        t.session.close

    # compiles a given regext and will returned a compiled regex
    # will logg and error and returnx None if regext can not be compiled
    def validate_regex(self, regex):
        compiled_regex = None

        try:
            compiled_regex = re.compile(regex, re.IGNORECASE)
        except Exception as e:
            logging.error("Invalid Regex: {}, Exception:\{}".format(regex, e))
            raise
        else:
            pass
        finally:
            pass
        return compiled_regex

    # id_regex is used to extract some meaning data FROM the name of the file like a year or month or date
    # that id gets stored with the meta data about the file to later use

    def insert_working_files(self, db, file_of_interest_obj, parent_file_id=0):
        assert isinstance(file_of_interest_obj, FilesOfInterest)
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        id_regex = file_of_interest_obj.file_name_data_regex
        # print("------insertworkingfile")

        for walked_filed_name in file_of_interest_obj.file_list:
            p = None
            extracted_id = None
            file_id = '0'

            # if get_mapped_table(walked_filed_name, self.file_pattern_list):
            if id_regex is not None:
                p = re.compile(id_regex)

            # apply regex pattern to extract data FROM the file name (date, month, year....etc...filename_2018-01-01.csv)
            if id_regex is not None:
                try:
                    extracted_id = p.findall(walked_filed_name)
                    if len(extracted_id) > 0:
                        file_id = extracted_id[0]
                except Exception as e:
                    logging.warning("No Embedded ID Found in FileName: id_REGEX = {}".format(id_regex))
            full_file_path = os.path.join(file_of_interest_obj.file_path, walked_filed_name)
            file_name = os.path.basename(full_file_path)
            file_path = os.path.dirname(full_file_path)

            row = db_table.db_table_def.MetaSourceFiles(file_path=file_path,
                                                        file_name=file_name,
                                                        file_name_data=file_id,
                                                        file_type=file_of_interest_obj.file_type,
                                                        parent_file_id=parent_file_id)
            t.add_record(row, commit=True)


    def dump_delimited_file(self, db, file_name, delimiter):
        shell_command = """psql -c "copy data_table FROM '{0}}' WITH DELIMITER AS '{1}' CSV QUOTE AS '"' """
        shell_command.format(file_name, delimiter)

    @staticmethod
    def reset_meta_table(db, option='FAILED', where_clause='1=1'):
        if option == 'ALL':
            db.update("""UPDATE logging.meta_source_files
                SET process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                WHERE  1=1 
                AND {}
                """.format(where_clause))
        if option == 'FAILED':
            logging.debug("RESET META DATA FAILED IMPORTS:")
            db.update("""UPDATE logging.meta_source_files
                set process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                WHERE  upper(file_process_state)='FAILED'
                AND {}
                """.format(where_clause))
        if option == 'RAW':
            logging.debug("RESET META DATA RAW IMPORTS:")
            db.update("""UPDATE logging.meta_source_files
                SET process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                WHERE  upper(file_process_state)='RAW'
                AND file_type in ('CSV','DATA')
                AND {}
                """.format(where_clause))
        if option == 'DATA':
            logging.debug("RESET META DATA   IMPORTS:")
            db.update("""UPDATE logging.meta_source_files
                SET process_start_dtm=null
                ,process_end_dtm=null
                ,current_worker_host=null
                ,current_worker_host_pid=null
                ,last_error_msg=null
                ,file_process_state='raw'
                WHERE   file_type in ('CSV','DATA')
                AND {}
                """.format(where_clause))
        db.commit()

    @staticmethod
    def walk_dir(foi, level=4):
        """Walks a directory structure and returns all files that match the regex pattern
        :rtype: FilesOfInterest
        """
        assert isinstance(foi, FilesOfInterest)

        file_path = foi.file_path

        regex = re.compile('zip')
        try:
            regex = re.compile(foi.regex)
        except Exception as e:
            logging.error("Bad Regex Pattern for Walking Directory: '{}'".format(foi.regex))
            raise

        if file_path[-1] != '/':
            file_path += '/'

        files_list = []
        ii = 0

        for root, subdirs, files in os.walk(file_path, topdown=True):
            if ii < level:
                for x in files:

                    rel_path = root.replace(file_path, "")
                    logging.debug("Walking Directory:{}:{}".format(subdirs, x))
                    if rel_path == "":
                        files_list.append(x)
                    else:
                        files_list.append(rel_path + "/" + x)

            ii += 1
        # logging.debug("Done Walking Directory:")
        match_list = list(filter(regex.match, files_list))
        logging.debug("Done Walking Directory:{}".format(list(match_list)))
        foi.file_list = match_list

        return foi

    # import one file at a time using client side copy command postgres
    # standard return will be sucesscode, rows_inserted,description
    def import_1file_client_side(self, foi, db):
        error_msg = None
        additional_msg = None
        assert isinstance(foi, FilesOfInterest)
        assert isinstance(db, db_utils.dbconn.Connection)
        self.ImporLogger = db_logging.logger.ImportLogger(db)
        data_file = foi.current_working_abs_file_name

        self.rows_inserted = 0
        # logging.debug("Into Import CopyCommand: {0}".format(dest.schema_name + "." + dest.table_name))
        if db is not None:

            # logging.debug("Into Import:{0}".format(dest.table_name))
            # if re.match(dest.regex, dest.full_file_name):
            cols = (foi.column_list)


            if foi.header_list_returned is not None:

                cols = ','.join(foi.header_list_returned)
            else:
                # remove file_id in the case we got headers from db

                cols = ','.join(cols)


            copy_string = None
            if foi.column_list is not None:

                copy_string = "{}({})".format(foi.schema_name + "." + foi.table_name, cols)
                # dest.column_list.replace(' ', '').replace('\n',  # '').strip(',')))
            else:
                copy_string = foi.schema_name + "." + foi.table_name
            logging.info("Import FROM file into: {}".format(copy_string))

            # not using this anymore because we don't know what order the file_id and crc columns are set in
            # that info will be returned from the process that has to append the file_id and crc
            # the header will be in the correct delimiter format
            # cols = db.get_columns(foi.table_name, foi.schema_name)

            header = ''
            if foi.has_header or foi.header_added:
                header = 'HEADER,'
            ###############THERE EXEC COMMAND LOGIC HERE########################################################
            envpwd = os.environ.get('PGPASSWORD', None)
            password = ''
            if envpwd is None:
                password = '-d {} '.format(db._database_name)

            copy_command_sql = "call op_dba.copy_from_host('{0}','{1}','DELIMITER ''|'' CSV')"
            copy_command_connection_flags = " -h {} -p {} -U {} {}".format(db._host, db._port, db._userid, password)
            copy_command_client_side = """psql {6} -c "\copy {0} FROM '{1}' with (format csv,{4} FORCE_NULL ({3}),delimiter '{2}', ENCODING '{5}')" """

            command_text = copy_command_client_side.format(
                copy_string,
                data_file,
                foi.file_delimiter,
                # ",".join(cols),
                cols,
                header,
                foi.encoding,
                copy_command_connection_flags)
            logging.info("Copy Command STARTED:{0}".format(foi.table_name))

            # vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

            bash_error_code, txt_out = commands.getstatusoutput(command_text)
            ###############THERE EXEC COMMAND LOGIC HERE########################################################

            if 'ERROR' in txt_out:
                logging.error(sys._getframe().f_code.co_name + " : " + txt_out)
                import_status = 'failed'

            logging.info("Command:{0}".format(command_text))
            logging.info("OUTPUT:{0} ".format(txt_out))

            # if txt_out[0] > 0 and not ('ERROR' in txt_out[1]):
            if int(bash_error_code) > 0:
                # self.flag_bad_file(db, self.source_file_path, self.curr_src_working_file, txt_out[1])
                error_msg = str(txt_out)[:2000]
                error_code = bash_error_code
                additional_msg = str(command_text)[:2000]

            else:
                self.processed_file_count += 1
                i = txt_out.split()
                self.rows_inserted = i[1]
                import_status = 'success'

        else:
            logging.debug("Regex Not Match Skipping:{0}".format(foi.table_name))
        status_dict = {}
        status_dict['rows_inserted'] = self.rows_inserted
        status_dict['import_status'] = import_status
        status_dict['error_msg'] = error_msg
        status_dict['additional_info'] = additional_msg

        return status_dict

    def match_regex(self, regex, folder_regex=None):
        t = False

        if re.match(regex, self.curr_src_working_file, re.IGNORECASE):
            if folder_regex is not None:
                if re.match(folder_regex, self.source_file_path, re.IGNORECASE):
                    t = True

            else:
                t = True
        return t

    # leveraging pandas libraries to read csv into a dataframe and let pandas
    # insert into database
    # @migrate_utils.static_func.timer
    @migrate_utils.static_func.dump_params
    def import_file_pandas(self, foi, db, lowercase=True, limit_rows=None, chunk_size=10000):

        full_file_path = None
        self.rows_inserted = 0
        import_status = None
        additional_info = None
        header = 'infer'
        names = None
        error_msg = None
        if db is not None:
            sqlalchemy_conn, meta = db.connect_sqlalchemy()

            table_name = foi.table_name
            counter = 0
            if lowercase:
                table_name = str.lower(str(table_name))
            try:

                if limit_rows is not None:
                    logging.debug("Pandas Read Limit SET: {0}:ROWS".format(limit_rows))
                if foi.has_header:
                    header = 'infer'
                    names = None
                else:
                    header = 0
                    # names = ','.join(foi.column_list)
                    # names = ','.join(foi.header_list_returned)
                    if foi.header_list_returned is not None:
                        names = foi.header_list_returned
                    else:
                        names = foi.column_list

                logging.debug(sys._getframe().f_code.co_name + " : " + foi.current_working_abs_file_name)
                dataframe_columns=''
                for counter, dataframe in enumerate(
                        pd.read_csv(foi.current_working_abs_file_name, delimiter=foi.file_delimiter, nrows=limit_rows,
                                    quotechar='"', chunksize=chunk_size, header=header,dtype=object,skiprows=foi.start_row)):

                    if not foi.has_header and len(foi.column_list)>0:
                        dataframe.columns = map(str,
                                                # foi.column_list
                                                names
                                                )  # dataframe.columns = map(str.lower, dataframe.columns)  # print("----- printing3",dest.column_list, dataframe.columns)
                    logging.debug(
                        "Pandas Insert Into DB: {0}->{1}-->Records:{2}".format(foi.schema_name, foi.table_name,
                                                                               counter * chunk_size))
                    ####################################################################################################
                    dataframe.to_sql(table_name, sqlalchemy_conn, schema=foi.schema_name, if_exists='append',
                                     index=False, index_label=names)
                    ####################################################################################################
                if counter == 0:
                    self.rows_inserted = counter * chunk_size + (len(dataframe))
                else:
                    self.rows_inserted = counter * chunk_size

                import_status = 'success'
                dataframe_columns=dataframe.columns.tolist()
            except Exception as e:

                cols_tb = db.get_table_columns(str.lower(str(foi.table_name)))
                delta = diff_list(dataframe_columns, cols_tb)
                cols = list(delta)
                if len(cols) > 1:
                    cols = str(list(delta))
                logging.error("ERROR: {0}".format(e))
                error_msg = str(e)[:256]
                import_status = 'failed'
                additional_info = (','.join(cols) + str(e))[:2000]
                db.commit()

        status_dict = {}
        status_dict['rows_inserted'] = self.rows_inserted
        status_dict['import_status'] = import_status
        status_dict['error_msg'] = error_msg
        status_dict['additional_info'] = additional_info
        return status_dict

    def import_files_copy_cmd(self, dest, db):
        """ this won't be used right now, using copy command instead

        for file in files.list:
            if re.match(dest.regex, file):
                print 'Match', files.path+'/'+file
                import_csv(dest.schema_name, dest.table_name,
                           files.path+'/'+file, dest.column_list)
        """

        if db is not None:
            for file_name in self.files.list:
                if re.match(dest.regex, file_name, re.IGNORECASE):
                    full_file_path = self.files.path + '/' + file_name
                    relative_file_path = full_file_path.replace(self.copy_command_root_path, "", 1)
                    sql_string = self.copy_command_sql.format(dest.table_name, relative_file_path)
                    logging.debug(sql_string)
                    db.execute(sql_string, False)
                    logging.info("Copy Command Compleated: {0}->{1}".format(self.curr_src_working_file, file_name))
                self.processed_file_count += 1

    def cleanup_files(self):
        if self.curr_file_success:
            if self.work_file_type == 'DATA':
                logging.debug("Deleting File:{0}".format(self.source_file_path + '/' + self.curr_src_working_file))
                os.remove(self.source_file_path + '/' + self.curr_src_working_file)
                if not os.listdir(self.source_file_path):
                    os.rmdir(self.source_file_path)

    def insert_prepend(self, orgfile, newfile, string_data):
        with open(orgfile, 'r') as f:
            with open(newfile, 'w') as f2:
                f2.write(string_data + f.read())

    def insert_append(self, orgfile, string_data):
        with open(orgfile, 'w') as f:
            f.write(self.source_file_path)

    def list_current_work(db, host=None):
        assert isinstance(db, db_utils.dbconn)
        if host is not None:
            logging.info(db.query("""SELECT file_name, current_worker_host FROM meta_source_files,id
                        WHERE  current_worker_host is not null and current_worker_host_pid is not null
                        ORDER BY process_start_dtm"""))
        else:
            logging.info(db.query("""SELECT file_name, current_worker_host FROM meta_source_files,id
                        WHERE  current_worker_host like '%{}%' and current_worker_host_pid is not null
                        ORDER BY process_start_dtm""".format(host)))

    # process_error HAS to equal 'sucess' to be marked as process
    # todo handle error when file is not longer located in directory because it was moved
    def finish_work(self, db, status_dict=None, file_of_interest=None, vacuum=True):

        assert isinstance(db, db_utils.dbconn.Connection)
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
        row = t.get_record(db_table.db_table_def.MetaSourceFiles.id == self.meta_source_file_id)
        row.process_end_dtm = datetime.datetime.now()
        print("type status_dict",status_dict)
        if status_dict is None:
            row.file_process_state = 'Uknown ERR'
        elif status_dict.get('import_status',None)== 'success':
            row.file_process_state = 'Processed'
            if file_of_interest is not None:
                row.database_table = ".".join([str(file_of_interest.schema_name), str(file_of_interest.table_name)])
            row.rows_inserted = self.rows_inserted
            self.curr_file_success=True
        else:
            row.last_error_msg = str(status_dict)[:2000]
            row.file_process_state = 'Failed'
            row.rows_inserted = 0


        if vacuum and file_of_interest is not None and status_dict is None:
            db.vacuum(file_of_interest.schema_name, file_of_interest.table_name)
        t.session.commit()
        status_dict = None
        return t.session.close()

    # @migrate_utils.static_func.timer
    def get_work(self, db):
        assert isinstance(db, db_utils.dbconn.Connection)
        assert isinstance(self.foi_list, list)
        self.processed_file_count = 0
        self.total_data_file_count = 0
        self.curr_file_success = False  # reset status of file
        t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)

        # to ensure we lock 1 row to avoid race conditions
        t.engine.execute(("""
                    UPDATE {0}.meta_source_files SET
                    current_worker_host='{1}', current_worker_host_pid={2}, process_start_dtm=now()
                    WHERE (file_path ||file_name) in (select file_path ||file_name
                        FROM {0}.meta_source_files WHERE  current_worker_host is null order by
                        id asc, file_size asc, file_name_data desc ,file_type asc limit 1)
                    """).format(db_table.db_table_def.MetaSourceFiles.DbSchema, self.host, self.curr_pid))
        t.session.commit()

        row = t.get_record(db_table.db_table_def.MetaSourceFiles.current_worker_host == self.host,
                           db_table.db_table_def.MetaSourceFiles.current_worker_host_pid == self.curr_pid,
                           db_table.db_table_def.MetaSourceFiles.process_end_dtm == None)

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
            self.append_file_id = row.id
            self.row_count = row.total_rows

            try:

                self.file_size = os.path.getsize(self.source_file_path + '/' + self.curr_src_working_file)

                row.file_size = self.file_size
                t.session.commit()
                if self.work_file_type in ('DATA', 'CSV') and self.row_count == 0:
                    # logging.debug("Working DATAFILE:{0}:".format(self.curr_src_working_file))
                    row.total_files = 1
                    logging.debug(
                        "Counting File : {}".format(os.path.join(self.source_file_path, self.curr_src_working_file)))
                    self.row_count,dummy = migrate_utils.static_func.count_csv(
                        os.path.join(self.source_file_path, self.curr_src_working_file))
                    row.total_rows = self.row_count
                    logging.debug("Counting File Result:{0}:".format(self.row_count))


            except Exception as e:
                # print(type(e))
                row.last_error_msg = str(e)[:2000]

                logging.debug("Flagging Bad File: {}".format(self.curr_src_working_file))
                logging.error(e)
                status_dict = {}
                status_dict['import_status'] = 'failed'
                status_dict['error_msg'] = e

                self.finish_work(db, status_dict=status_dict, file_of_interest=None, vacuum=True)

            else:
                pass
            finally:

                t.session.commit()
                t.session.close()

        return self.curr_src_working_file

    def extract_file(self, db, full_file_path, abs_writable_path):
        status_dict = {}
        try:
            self.files = zip_utils.unzipper.extract_file(full_file_path, abs_writable_path, False, self.work_file_type)
            t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
            row = t.get_record(db_table.db_table_def.MetaSourceFiles.id == self.meta_source_file_id)
            self.total_files = len(self.files)

            row.total_files = self.total_files
            t.session.commit()

        # We walk the tmp dir and add those data files to list of to do
            new_src_dir = abs_writable_path
            logging.debug("WALKING EXTRACTED FILES:\src_dir:{0}\nworking_dir:{1}:".format(new_src_dir, self.working_path))

            file_table_map = [FilesOfInterest('DATA', '', file_path=abs_writable_path, file_name_data_regex=None,
                                          parent_file_id=self.meta_source_file_id)]

            DataFile(new_src_dir, db, file_table_map, parent_file_id=self.meta_source_file_id)
        except Exception as e:
            status_dict['import_status'] = 'failed'
            status_dict['error_msg'] = 'Error During Unziping File'
        else:
            status_dict['import_status'] = 'success'

        return status_dict

    # Do work will query the meta source table for a record
    # It will stamp that record with this pid and ip address
    # When it is done with the processing of the record it we stamp the process_end_dtm
    # signifying the file has been processed

    def do_work(self, db, cleanup=True, limit_rows=None, import_type=None, vacuum=True, chunksize=10000):
        df = self
        status_dict = {}

        while self.get_work(db) is not None:

            logging.debug("Got New Working File:{0}:".format(self.curr_src_working_file))

            if self.work_file_type in ('DATA', 'CSV'):
                # check the current file against our list of regex to see if it
                # matches any table mapping

                foi = get_mapped_table(self.curr_src_working_file, self.foi_list)

                logging.debug("Getting Mapped table:{}\n{}".format(self.curr_src_working_file, foi))
                if foi is not None:
                    # we found a table that is mapped to file of interest so we
                    foi.column_list = db.get_columns(foi.table_name, foi.schema_name)
                    # if 2 column_names are reserved file_id and crc

                    try:
                        # Todo Come back and clean this up
                        if foi.append_file_id is False:
                            foi.column_list.remove('file_id')
                        if foi.append_crc is False:
                            foi.column_list.remove('crc')
                        # logging.error("No Table Mapping Found Breaking Out:{}".format(self.curr_src_working_file))
                    except:
                        pass
                    if foi.insert_option and len(foi.column_list)>0:
                        db.truncate_table(foi.schema_name, foi.table_name)
                        print("Truncating Data:{}.{}".format(foi.schema_name, foi.table_name))
                    else:
                        pass
                        # print("Appending  Data:{}.{}".format(foi.schema_name, foi.table_name))
                    logging.debug("DATA-File:{}".format(self.curr_src_working_file))

                    # use the line below if we need to stamp the data file w/ a
                    # column that has additional data
                    foi.current_working_abs_file_name = os.path.join(self.source_file_path, self.curr_src_working_file)
                    header_added = None
                    if foi.append_file_id or foi.append_crc or foi.start_row>0:
                        # full_file_name = os.path.join(self.source_file_path, self.curr_src_working_file)
                        # print(self.working_path, "/appended/", self.curr_src_working_file)
                        ################################################################################################
                        new_file_name, header_added, header_list_returned = self.insert_into_file( foi,self.meta_source_file_id,db=db)
                        ################################################################################################
                        foi.working_path = os.path.dirname(new_file_name)
                        foi.current_working_abs_file_name = new_file_name
                    else:
                        print("not inserting anything to file",foi.append_crc,foi.append_file_id)
                    if header_added is not None:
                        foi.header_added = header_added
                        foi.header_list_returned = header_list_returned

                    # print(""df.row_count, min_row)
                    logging.debug("File Row Count:{}".format(df.row_count))

                    if df.match_regex(foi.regex, foi.folder_regex):
                        try:

                            if import_type == self.IMPORT_VIA_PANDAS:

                                ####################################################################################
                                status_dict = self.import_file_pandas(foi, db, limit_rows=limit_rows,
                                                                      chunk_size=chunksize)
                                ####################################################################################
                            # only postgres support for now
                            elif import_type == self.IMPORT_VIA_CLIENT_CLI:
                                ####################################################################################
                                status_dict = self.import_1file_client_side(foi, db)
                                ####################################################################################
                            elif import_type == self.IMPORT_VIA_CUSTOM_FUNC:
                                status_dict == self.CUSTOM_FUNC()
                            else:
                                raise Exception(
                                    "No Import Method Provided: \n{}\n{}".format(
                                        self.IMPORT_VIA_CLIENT_CLI,
                                        self.IMPORT_VIA_PANDAS))

                        except Exception as e:

                            logging_handler = db_logging.logger.ImportLogger(db)
                            error_log_entry = logging_handler.ErrorLog(
                                program_unit=sys.argv[0],
                                error_code=import_type,
                                error_message='Inside Function import_1file_client_side',
                                error_timestamp=datetime.datetime.now(),
                                user_name=db._userid,
                                sql_statement=str(e)[:2000])

                            error_log_entry.error_message = str(e)[:256]
                            error_log_entry.sql_statement = 'Import process Exception Raised'
                            error_log_entry.error_timestamp = datetime.datetime.now()
                            logging_handler.session.add(error_log_entry)

                            logging_handler.session.commit()
                            logging_handler.session.close()
                            status_dict['error_msg'] = str(e)[:2000]

                            logging.error("Unknown Error Occurred Importing: {}".format(e))

                        else:
                            logging_handler = db_logging.logger.ImportLogger(db)
                            if status_dict['import_status'] == 'success':
                                log_entry = logging_handler.LoadStatus(
                                    table_name=foi.table_name,
                                    program_unit=sys.argv[0],
                                    program_unit_type_code='python',
                                    file_path=foi.current_working_abs_file_name,
                                    records_inserted=status_dict['rows_inserted'],
                                    success=1, start_date=datetime.datetime.now(),
                                    end_date=datetime.datetime.now(),
                                    previous_record_count=0, current_record_count=0,
                                    records_updated=0,
                                    records_deleted=0, created_by=db._userid,
                                    created_date=datetime.datetime.now())
                                logging_handler.session.add(log_entry)
                                t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
                                row = t.get_record(
                                    db_table.db_table_def.MetaSourceFiles.id == self.meta_source_file_id)
                                row.rows_inserted = status_dict['rows_inserted']
                                t.add_record(row, commit=True)
                                t.session.commit()
                                t.session.close()
                            else:

                                logging_handler = db_logging.logger.ImportLogger(db)
                                error_log_entry = logging_handler.ErrorLog(
                                    program_unit=sys.argv[0],
                                    error_code=None,
                                    error_message='Inside Function import_1file_client_side',
                                    error_timestamp=None,
                                    user_name=db._userid, sql_statement=status_dict['additional_info'])

                                error_log_entry.error_message = status_dict['error_msg'][:256]
                                error_log_entry.sql_statement = status_dict['additional_info'][:2000]
                                error_log_entry.error_timestamp = datetime.datetime.now()
                                logging_handler.session.add(error_log_entry)

                            logging_handler.session.commit()
                            logging_handler.session.close()
                        finally:
                            assert isinstance(status_dict,dict)

                            df.finish_work(db, status_dict=status_dict, file_of_interest=foi,
                                           vacuum=True)

                # no matching pattern for regext and db_tablename
                else:
                    status_dict={}
                    status_dict['import_status'] = 'failed'
                    status_dict['error_msg'] = 'No Pattern Mapping Found'
                    df.finish_work(db, status_dict=status_dict, file_of_interest=foi, vacuum=vacuum)
            # Process Compressed files
            else:
                full_file_name = os.path.join(self.source_file_path, self.curr_src_working_file)

                status_dict=self.extract_file(db, full_file_name, os.path.join(self.working_path, self.curr_src_working_file))
                self.finish_work(db, status_dict=status_dict,vacuum=vacuum)
            if cleanup:
                self.cleanup_files()  # import_files(files,loan_acquisition)
