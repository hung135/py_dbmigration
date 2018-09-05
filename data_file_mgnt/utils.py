
import db_utils
import data_file_mgnt
import time
import logging as log
import re
import custom_logic.purge_temp_file as purge


logging = log.getLogger()
import_type = 'client_copy'

# list of key values to replace in sql with runtime data values


def inject_frame_work_data(sql, foi, df):
    #print("-----x-xxxx: ", type(sql), type("$$file_id$$"))
    x = sql.replace("{file_id}", str(df.meta_source_file_id))
    x = x.replace("{schema_name}", foi.schema_name)
    x = x.replace("{table_name}", foi.table_name)

    return x


def execute_sql(db, sql_list, foi, df):
    for id, sql in enumerate(sql_list):
        # print(sql['sql'], "executing sike", type(sql))
        logging.info("\tSQL Step #: {}".format(id))
        x = inject_frame_work_data(sql['sql'], foi, df)
        db.execute_permit_execption(x)

# pull the list of modules configured in the yaml file under process_logic
# it will execute each of the logic on this file in the order it was entered in the yaml file


def process_logic(foi, db, df):
    if foi.table_name_extract is not None:
        table_name_regex = re.compile(foi.table_name_extract)
        # table_name = table_name_regex.match(table_name))
        foi.table_name = re.search(foi.table_name_extract, df.curr_src_working_file).group(1)
    set_sql = "update logging.meta_source_files set reprocess={} where id={}".format(foi.reprocess, df.meta_source_file_id)
    db.execute_permit_execption(set_sql)

    process_logic = foi.process_logic
    if foi.pre_action is not None:
        logging.info("Executing Pre Load SQL")
        execute_sql(db, foi.pre_action, foi, df)
    continue_next_process = False

    for logic in process_logic:

        custom_logic = logic['logic']
        logic_name = custom_logic.split('.')[-1]
        # dynmaically import the modeul specified in the yaml file
        # this could be faster if we imported this once but for now it stays here
        logging.debug('Importing Module: {}'.format(custom_logic))
        module = __import__(custom_logic)
        # print(dir(module))

        imp = getattr(module, logic_name)
        logging.info('\t->Dynamic Module Start: {}'.format(custom_logic))
        df.set_work_file_status(db, df.meta_source_file_id, custom_logic)

        # maybe create a history table instead but for now cram into 1 field
        sql_set_process_trail = "update logging.meta_source_files set process_msg_trail=concat('{};\n',process_msg_trail) where id={}".format(
            logic_name, df.meta_source_file_id)
        db.execute_permit_execption(sql_set_process_trail)
        try:
            continue_next_process = imp.process(db, foi, df)
        except ValueError as e:
            df.set_work_file_status(db, df.meta_source_file_id, custom_logic, '{}: {}'.format(custom_logic, e))
        logging.debug('\t->Dynamic Module Ended: {}'.format(custom_logic))

        if not continue_next_process:
            logging.error('\t->Abort Processing for this file Because of Error: {}'.format(df.curr_src_working_file))
            df.set_work_file_status(db, df.meta_source_file_id, 'FAILED', custom_logic)
            break

    # if everything was kosher else file should have been tailed 'FAILED'
    if continue_next_process:
        if foi.post_action is not None:
            logging.info("Executing Post Load SQL")
            execute_sql(db, foi.post_action, foi, df)

        df.set_work_file_status(db, df.meta_source_file_id, 'Processed')
    purge.process(db, foi, df)
