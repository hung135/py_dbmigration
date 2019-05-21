
import logging
import os
import sys
import db_utils
import data_file_mgnt
import db_logging
import db_table
import migrate_utils
import commands


import pprint

logging.basicConfig(level='DEBUG')

# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params


# import one file at a time using client side copy command postgres
# standard return will be sucesscode, rows_inserted,description

# def process(db, file, file_id, dbschema):
def process(db, foi, df):
    continue_processing = False
    error_msg = None
    additional_msg = None
    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.dbconn.Connection)


    rows_inserted = 0
    import_status = None
    additional_info = None
    dataframe_columns = ''
    data_file = file = os.path.join(df.source_file_path, df.curr_src_working_file)
    limit_rows = foi.limit_rows
    table_name = foi.table_name
    target_schema = foi.schema_name
    file_id = df.meta_source_file_id
    header = foi.header_row
    names = foi.header_list_returned or foi.column_list
    cols = foi.column_list or db.get_columns(table_name, target_schema)
    encoding = foi.encoding
    
    column_count=len(cols)
     
    #count_column_csv(full_file_path, header_row_location=0, sample_size=200, delimiter=','):
    logging.debug("Delimiter: {}".format(foi.file_delimiter))
    file_column_count=migrate_utils.static_func.count_column_csv(data_file,header,10,foi.file_delimiter)


    if column_count!=file_column_count:
        logging.info('Using column_list2 since column counts differr:')
        logging.info('Config Column Count:{} Datafile Column Count: {}'.format(column_count,file_column_count))
        cols=foi.mapping.get('column_list2').split(',')

    if foi.header_list_returned is not None:

        cols = ','.join(foi.header_list_returned)
    else:
        # remove file_id in the case we got headers from db
        cols = ','.join(cols)
     
    ImporLogger = db_logging.logger.ImportLogger(db)
    header = ''
    if foi.use_header or foi.header_added:
        header = 'HEADER,'
    delim = foi.file_delimiter
    if foi.new_delimiter is not None:
        delim = foi.new_delimiter

    # logging.debug("Into Import CopyCommand: {0}".format(dest.schema_name + "." + dest.table_name))
    if db is not None:

        # logging.debug("Into Import:{0}".format(dest.table_name))
        # if re.match(dest.regex, dest.full_file_name):

        copy_string = None
        if cols is not None:

            copy_string = "{}({})".format(target_schema + "." + table_name, cols)
            # dest.column_list.replace(' ', '').replace('\n',  # '').strip(',')))
        else:
            copy_string = target_schema + "." + table_name
        logging.debug("\t\tImport FROM file into: {}, {}.{}".format(copy_string, target_schema, table_name))

        # not using this anymore because we don't know what order the file_id and crc columns are set in
        # that info will be returned from the process that has to append the file_id and crc
        # the header will be in the correct delimiter format
        # cols = db.get_columns(foi.table_name, foi.schema_name)

        ###############THERE EXEC COMMAND LOGIC HERE########################################################
        envpwd = os.environ.get('PGPASSWORD', None)
        password = ''
        if envpwd is None:
            password = '-d {} '.format(db._database_name)

        copy_command_sql = "call op_dba.copy_from_host('{0}','{1}','DELIMITER ''|'' CSV')"
        copy_command_connection_flags = " -h {} -p {} -U {} {}".format(db._host, db._port, db._userid, password)
        copy_command_client_side = """psql {6} -c "\copy {0} FROM '{1}' with (format csv,{4} FORCE_NULL ({3}),delimiter '{2}', ENCODING '{5}')" """

        command_text = copy_command_client_side.format(
            copy_string.replace('"', ''),
            data_file,
            delim,
            # ",".join(cols),
            cols,
            header,
            encoding,
            copy_command_connection_flags)
        logging.info("\t\tCopy Command STARTED: {0}".format(table_name))
        bash_error_code, txt_out = commands.getstatusoutput(command_text)

        ###############THERE EXEC COMMAND LOGIC HERE########################################################

        if 'ERROR' in txt_out:
            logging.error(sys._getframe().f_code.co_name + " : " + txt_out)
        else:
            continue_processing = True
        logging.debug("\t\tCommand: {0}".format(command_text))
        logging.info("\t\tOUTPUT: {0} ".format(txt_out))
        logging.info("\t\tCopy Command Completed: {0}".format(table_name))
        # if txt_out[0] > 0 and not ('ERROR' in txt_out[1]):
        if int(bash_error_code) > 0:
            error_msg = str(txt_out)[:2000]
            df.load_status_msg = error_msg
            error_code = bash_error_code
            additional_msg = str(command_text)[:2000]

        else:
            i = txt_out.split()
            rows_inserted = i[1]

    # set values into meta_source_files table
    t = db_table.db_table_func.RecordKeeper(db, db_table.db_table_def.MetaSourceFiles)
    row = t.get_record(db_table.db_table_def.MetaSourceFiles.id == file_id)
    row.rows_inserted = rows_inserted
    row.database_table = target_schema + '.' + table_name
    row.last_error_msg = ( error_msg or '')+'\n'+str(row.last_error_msg or '')
    t.session.commit()
    t.session.close()


    return continue_processing
