import yaml
import os
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as dfm
import py_dbmigration.migrate_utils.static_func as static_func

import pprint


import logging as log
log.basicConfig()
logging = log.getLogger()
logging.setLevel(log.INFO)
logging.setLevel(log.DEBUG)


def merge_two_dicts(x, y):
    # stolen from here
    # https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def process_yaml(yaml_file=None):
    if yaml_file is None:
         
        yaml_file = os.path.join(os.getcwd(),"data_load.yaml")
    logging.debug("loaind yaml file: {}".format(yaml_file))
    paths = yaml.full_load(open(yaml_file))

    datafiles = []
    mapping_counter = 0
    try:
        for path_dict in paths:
            file_path = path_dict['path']

            enabled = path_dict.get('enabled', True)
            if enabled:

                mapping = path_dict['mapping']
                project_name = path_dict['project_name']

                table_name = path_dict.get('table_name', None)
                reset_failed = path_dict.get('reset_failed', False)

                file_delimiter = path_dict.get('file_delimiter', None)
                column_list = path_dict.get('column_list')
                schema_name = path_dict.get('schema_name', None)
                use_header = path_dict.get('use_header', False)
                has_header = path_dict.get('has_header', True)
                append_file_id = path_dict.get('append_file_id', False)
                upsert_function_name = path_dict.get('upsert_function_name', None)
                encoding = path_dict.get('file_encoding', 'UTF8')
                header_row_location = path_dict.get('header_row_location', None)
                quoted_header = path_dict.get('quoted_header', True)
                import_method = path_dict.get('import_method', 'IMPORT_VIA_PANDAS')
                unzip_again = path_dict.get('unzip_again', False)
                pre_action_sql = path_dict.get('pre_action_sql', None)
                limit_rows = path_dict.get('limit_rows', None)
                new_delimiter = path_dict.get('new_delimiter', None)
                reprocess = path_dict.get('reprocess', True)

                table_name_extract = path_dict.get('table_name_extract', None)
                global_process_logic = path_dict.get('process_logic', None)

                for id, regex_dict in enumerate(mapping):
                    item_enabled = regex_dict.get('enabled', enabled)
                    if item_enabled:
                        file_regex = regex_dict['file_regex']
                        file_type = regex_dict['file_type']
                        table_name = regex_dict.get('table_name', table_name)
                        #print("---item enabled ", item_enabled, file_regex)
                        file_delimiter = regex_dict.get('file_delimiter', file_delimiter)
                        column_list = regex_dict.get('column_list', column_list)
                        schema_name = regex_dict.get('schema_name', schema_name)
                        use_header = regex_dict.get('use_header', use_header)
                        has_header = regex_dict.get('has_header', has_header)
                        append_file_id = regex_dict.get('append_file_id', append_file_id)
                        upsert_function_name = regex_dict.get('upsert_function_name', upsert_function_name)
                        encoding = regex_dict.get('file_encoding', encoding)
                        header_row_location = regex_dict.get('header_row_location', header_row_location)
                        quoted_header = regex_dict.get('quoted_header', quoted_header)
                        import_method = regex_dict.get('import_method', import_method)
                        unzip_again = regex_dict.get('unzip_again', unzip_again)
                        pre_action_sql = regex_dict.get('pre_action_sql', pre_action_sql)
                        limit_rows = limit_rows
                        new_delimiter = regex_dict.get('new_delimiter', new_delimiter)
                        table_name_extract = regex_dict.get('table_name_extract', table_name_extract)
                        # for key, val in regex_dict.iteritems():
                        # if not val == 'None':
                        # print(key, val)
                        limit_rows = regex_dict.get('limit_rows', None)
                        reprocess_file = regex_dict.get('reprocess', reprocess)
                        post_action = regex_dict.get('post_action', None)
                        pre_action = regex_dict.get('pre_action', None)
                        logic = regex_dict.get('process_logic', global_process_logic)

                        # process_logic=merge_two_dicts(global_process_logic,logic)
                        process_logic = ((global_process_logic or []) + (logic or []))
                        #print(" logic \n:", process_logic)
                        if limit_rows is None or not limit_rows.isdigit():
                            limit_rows = None
                        if item_enabled:
                            datafiles.append(dfm.data_files.FilesOfInterest(file_type=file_type,
                                                                            table_name=table_name,
                                                                            file_path=file_path,
                                                                            file_regex=file_regex,
                                                                            file_delimiter=file_delimiter,
                                                                            column_list=column_list,
                                                                            schema_name=schema_name,
                                                                            use_header=use_header,
                                                                            has_header=has_header,
                                                                            append_file_id=append_file_id,
                                                                            upsert_function_name=upsert_function_name,
                                                                            encoding=encoding,
                                                                            header_row_location=header_row_location,
                                                                            quoted_header=quoted_header,
                                                                            import_method=import_method,
                                                                            unzip_again=unzip_again,
                                                                            pre_action_sql=pre_action_sql,
                                                                            limit_rows=limit_rows,
                                                                            new_delimiter=new_delimiter,
                                                                            post_action=post_action,
                                                                            pre_action=pre_action,
                                                                            process_logic=process_logic,
                                                                            project_name=project_name,
                                                                            table_name_extract=table_name_extract,
                                                                            reprocess=reprocess_file, yaml=path_dict,mapping=regex_dict)
                                             )
                            mapping_counter += 1
                            logging.debug("{} Regex Pattern Configured: {} \n\t\tFile Type: {}".format(
                                mapping_counter, regex_dict['file_regex'], regex_dict['file_type']))
                        else:
                            logging.debug("Configruation Disabled for Path: \t{} ".format(file_path))

    except Exception as e:
        logging.error("Error processing YAML Files: {}\n Error Message: {}".format(yaml_file, e))
        datafiles = []
    return datafiles

def main(yamlfile=None,write_path=None,schema=None):


    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--yaml')
    parser.add_argument('--logging')
    args = parser.parse_args()
    if args.logging is 'debug':
 
        logging.setLevel(log.DEBUG)
    else:
 
        logging.setLevel(log.INFO)

    datafiles =None
    if args.yaml is not None:
        datafiles = process_yaml(os.path.abspath(args.yaml))
    else:
        datafiles = process_yaml(yamlfile)
 
    writable_path = os.getenv('WORKINGPATH',write_path) or os.getcwd()
    PGDATASCHEMA = os.getenv('PGDATASCHEMA',schema)
    
    if len(datafiles) > 0:
        db = db_utils.DB(schema=PGDATASCHEMA)
         
        # db.truncate_table("logging", "meta_source_files")

        df = dfm.data_files.DataFile(writable_path, db, datafiles)
        df.init_db()
        df.reset_meta_table(db, 'FAILED', where_clause=" (1=1) ")

        df.do_work(db, cleanup=False,    skip_ifexists=False)
        db.execute('vacuum analyze logging.meta_source_files')
    else:
        logging.info("No configruation Items found...Exiting.")

if __name__ == '__main__':
    main()