
import db_utils
import data_file_mgnt
import time
import logging as log


logging = log.getLogger()
import_type = 'client_copy'

# list of key values to replace in sql with runtime data values


def inject_frame_work_data(sql, foi, df):
    #print("-----x-xxxx: ", type(sql), type("$$file_id$$"))
    x = sql.replace("{file_id}", str(df.meta_source_file_id))
    x = x.replace("{schema_name}", foi.schema_name)
    x = x.replace("{table_name}", foi.table_name)
    print(x)
    return x


def execute_sql(db, sql_list, foi, df):
    for sql in sql_list:
        # print(sql['sql'], "executing sike", type(sql))
        x = inject_frame_work_data(sql['sql'], foi, df)
        db.execute(x)

# pull the list of modules configured in the yaml file under process_logic
# it will execute each of the logic on this file in the order it was entered in the yaml file


def process_logic(foi, db, df):

    process_logic = foi.process_logic
    if foi.pre_action is not None:
        logging.info("Executing Pre Load SQL")
        execute_sql(db, foi.pre_action)
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
        logging.debug('\t->Started Processing Module: {}'.format(custom_logic))
        df.set_work_file_status(db, df.meta_source_file_id, custom_logic)
        try:
            continue_next_process = imp.process(db, foi, df)
        except ValueError as e:
            df.set_work_file_status(db, df.meta_source_file_id, custom_logic, '{}: {}'.format(custom_logic, e))
        logging.debug('\t->Completed Processing Module: {}'.format(custom_logic))

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