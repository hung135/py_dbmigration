
 
from py_dbmigration import migrate_utils 
import datetime
import logging as log
import re
from py_dbmigration.custom_logic import purge_temp_file as purge
from  py_dbmigration import data_file_mgnt


logging = log.getLogger()
 

# list of key values to replace in sql with runtime data values


def inject_frame_work_data(sql, foi, df):
     
    x = sql.replace("{file_id}", str(df.meta_source_file_id))
    x = x.replace("{schema_name}", foi.schema_name)
    x = x.replace("{table_name}", foi.table_name)
     
    
    x = x.replace("{column_list}",','.join(foi.column_list or []))

    return x


def execute_sql(db, sql_list, foi, df):

    for id, sql in enumerate(sql_list):
        # print(sql['sql'], "executing sike", type(sql))

        modified_sql = inject_frame_work_data(sql['sql'], foi, df)
        shorten_sql = (modified_sql[:50] + "...") if len(modified_sql) > 75 else modified_sql
        logging.info("\tSQL Step #: {} {}".format(id, shorten_sql))

        t = datetime.datetime.now()
         
        db.execute(modified_sql,catch_exception=False)
        
        time_delta = (datetime.datetime.now() - t)
        logging.info("\t\tExecution Time: {}sec".format(time_delta))

# pull the list of modules configured in the yaml file under process_logic
# it will execute each of the logic on this file in the order it was entered in the yaml file


def process_logic(foi, db, df):
 
    #store result of action you do in this variable
    df.load_status_msg = None
    if foi.table_name_extract is not None:
        table_name_regex = re.compile(foi.table_name_extract)
        # table_name = table_name_regex.match(table_name))
        #print(foi.table_name_extract, df.curr_src_working_file)
        foi.table_name = migrate_utils.static_func.convert_str_snake_case(re.search(foi.table_name_extract, df.curr_src_working_file).group(1))

        #print(foi.table_name)
    
    set_sql = "update logging.meta_source_files set reprocess={} where id={}".format(foi.reprocess, df.meta_source_file_id)
    db.execute(set_sql,catch_exception=False)

    process_logic = foi.process_logic
    if foi.pre_action is not None:
        logging.info("Executing Pre Load SQL")
        execute_sql(db, foi.pre_action, foi, df)
    continue_next_process = False

    for logic in process_logic:

        custom_logic = logic['logic']
        logic_name = custom_logic.split('.')[-1]
        fqn_logic = 'py_dbmigration.{}'.format(custom_logic)
        # dynmaically import the modeul specified in the yaml file
        # this could be faster if we imported this once but for now it stays here
        logging.debug('Importing Module: {}'.format(custom_logic))
         
        module = __import__('py_dbmigration.custom_logic',fromlist=[logic_name])
        #print(dir(module))

        imp = getattr(module, logic_name)
         
        logging.info('\t->Dynamic Module Start: {}'.format(custom_logic))
        df.set_work_file_status(db, df.meta_source_file_id, custom_logic)

        # maybe create a history table instead but for now cram into 1 field
        sql_set_process_trail = "update logging.meta_source_files set process_msg_trail=concat('{};\n',process_msg_trail) where id={}".format(
            logic_name, df.meta_source_file_id)
        db.execute(sql_set_process_trail,catch_exception=False)
        try:
            
            t = datetime.datetime.now()
             
            logic_status = imp.process(db, foi, df)
            try: 
                
                assert isinstance(logic_status,data_file_mgnt.data_files.Status)
                continue_next_process=logic_status.continue_processing
              
            except Exception as e:
                logging.warning("Please implement Status Object for this custom logic")
                continue_next_process=logic_status
            
            time_delta = (datetime.datetime.now() - t)
            logging.info("\t\t\tExecution Time: {}sec".format(time_delta))
             
        except Exception as e:
            df.set_work_file_status(db, df.meta_source_file_id, custom_logic, '{}: {}'.format(custom_logic, e))
            logging.error("Unexpected Error occured running Custom logic: {}".format(e))
            
        logging.debug('\t->Dynamic Module Ended: {}'.format(custom_logic))

        if not continue_next_process:
            logging.error('\t->Abort Processing for this file Because of Error: {}'.format(df.curr_src_working_file))
            df.set_work_file_status(db, df.meta_source_file_id, 'FAILED', custom_logic+'\n'+str(df.load_status_msg or ''))
            break

    # if everything was kosher else file should have been tailed 'FAILED'
    if continue_next_process:
        if foi.post_action is not None:
            logging.info("Executing Post Load SQL")
            execute_sql(db, foi.post_action, foi, df)

        df.set_work_file_status(db, df.meta_source_file_id, 'Processed')
    purge.process(db, foi, df)
