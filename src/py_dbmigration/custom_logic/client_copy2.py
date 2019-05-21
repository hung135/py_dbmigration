
import logging
import os
import sys
import db_utils
import data_file_mgnt
import db_logging
import commands
 

import pprint

logging.basicConfig(level='DEBUG')

# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params




# import one file at a time using client side copy command postgres
# standard return will be sucesscode, rows_inserted,description
def import_file(db, foi):
    error_msg = None
    additional_msg = None
    assert isinstance(foi, data_file_mgnt.data_files.FilesOfInterest)
    assert isinstance(db, db_utils.DB)



    rows_inserted = 0
    import_status = None
    additional_info = None
    dataframe_columns = ''
    data_file = foi.current_working_abs_file_name
    limit_rows = foi.limit_rows
    table_name = foi.table_name
    target_schema = foi.schema_name
    header = foi.header_row
    names = foi.header_list_returned or foi.column_list
    cols = foi.column_list
    encoding = foi.encoding
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
        logging.info("Import FROM file into: {}, {}.{}".format(copy_string, target_schema, table_name))

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
        logging.info("Copy Command STARTED:{0}".format(table_name))

        # vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv

        bash_error_code, txt_out = commands.getstatusoutput(command_text)
        ###############THERE EXEC COMMAND LOGIC HERE########################################################

        if 'ERROR' in txt_out:
            logging.error(sys._getframe().f_code.co_name + " : " + txt_out)
            import_status = 'FAILED'

        logging.info("Command:{0}".format(command_text))
        logging.info("OUTPUT:{0} ".format(txt_out))

        # if txt_out[0] > 0 and not ('ERROR' in txt_out[1]):
        if int(bash_error_code) > 0:
            
            error_msg = str(txt_out)[:2000]
            error_code = bash_error_code
            additional_msg = str(command_text)[:2000]

        else:
            
            i = txt_out.split()
            rows_inserted = i[1]
            import_status = 'success'

    else:
        logging.debug("Regex Not Match Skipping:{0}".format(table_name))
    status_dict = {}
    status_dict['rows_inserted'] = rows_inserted
    status_dict['import_status'] = import_status
    status_dict['error_msg'] = error_msg
    status_dict['additional_info'] = additional_msg

    return status_dict
