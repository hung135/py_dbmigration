
 
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *
import os
import datetime
import logging as log
import re
import sys
from py_dbmigration.custom_logic import purge_temp_file as purge
import yaml
import py_dbmigration.db_table as db_table

logging = log.getLogger()


# list of key values to replace in sql with runtime data values


def recurse_replace_yaml(p_trg_data, p_base_dict):
    def inject_yaml_data(str_data, yaml):

        dict_keys = yaml.keys()
        # print('before ', str_data)
        for key in dict_keys:
            if isinstance(yaml[key], dict):
                str_data=recurse_replace_yaml(str_data,yaml[key])
            elif not (isinstance(yaml[key], list) or isinstance(yaml[key], dict)):
                str_data = str_data.replace("{{" + key + "}}", str(yaml[key]))
        # print('after ', str_data)
        return str_data

    assert isinstance(p_base_dict, dict), "2nd parameter has to be TYPE dict: {}".format(type(p_base_dict))
    if isinstance(p_trg_data, list):
        new_list = []
        for trg_item in p_trg_data:
            if isinstance(trg_item, str):
                trg_item = inject_yaml_data(trg_item, p_base_dict)
            elif isinstance(trg_item, list) or isinstance(trg_item, dict):
                if isinstance(trg_item, dict):
                    trg_item = recurse_replace_yaml(trg_item, trg_item)
                trg_item = recurse_replace_yaml(trg_item, p_base_dict)
            new_list.append(trg_item)
        p_trg_data = new_list
        # pprint.pprint(p_trg_data)
    elif isinstance(p_trg_data, dict):
        # v_trg_dict = copy.deepcopy(p_trg_dict)
        # pprint.pprint(p_trg_data)
        new_dict = {}
        for trg_key in p_trg_data.keys():
            trg_item = p_trg_data[trg_key]
            if isinstance(trg_item, str):
                if trg_key=='logging_table':
                    print(trg_item)
                    import pprint 
                    pprint.pprint(p_base_dict)
                trg_item = inject_yaml_data(trg_item, p_base_dict)
            elif isinstance(trg_item, list) or isinstance(trg_item, dict):
                trg_item = recurse_replace_yaml(trg_item, p_base_dict)
            new_dict[trg_key] = trg_item
        p_trg_data = new_dict
    elif isinstance(p_trg_data, str):
        p_trg_data = inject_yaml_data(p_trg_data, p_base_dict)

    return p_trg_data
# run through the yaml and replace embedded params
def pre_process_yaml(yaml_file):
    # yaml_file = os.path.abspath(yaml_file)
    yaml_data=None
    with open(yaml_file,'r') as f:
        yaml_data = yaml.full_load(f)
     
    mig_list = []
    for yaml_obj in yaml_data:
        item = recurse_replace_yaml(yaml_obj, yaml_obj)
        mig_list.append(item)
        for i in item.keys():
            j = item[i]
            if isinstance(j, dict):

                j = recurse_replace_yaml(j, j)

            elif isinstance(j, list):

                j = recurse_replace_yaml(j, yaml_obj)

            elif isinstance(j, str):

                j = recurse_replace_yaml(j, yaml_obj)

    #pprint.pprint(mig_list)
     
    return mig_list
def inject_frame_work_data(string_text, foi, df):

    x = string_text.replace("{{{file_id}}}", str(df.meta_source_file_id))
    x = x.replace("{{schema_name}}", str(foi.schema_name or 'NONE'))
    x = x.replace("{{table_name}}", str(foi.table_name or 'NONE'))
    x = x.replace("{{column_list}}", ','.join(foi.column_list or []))

    return x


def execute_sql(db, sql_list, foi, df):

    for id, sql in enumerate(sql_list):
        # print(sql['sql'], "executing sike", type(sql))

        modified_sql = inject_frame_work_data(sql['sql'], foi, df)
        shorten_sql = (
            modified_sql[:50] + "...") if len(modified_sql) > 75 else modified_sql
        logging.info("\tSQL Step #: {} {}".format(id, shorten_sql))

        t = datetime.datetime.now()

        db.execute(modified_sql, catch_exception=False)
        

        time_delta = (datetime.datetime.now() - t)
        logging.info("\t\tExecution Time: {}sec".format(time_delta))

# pull the list of modules configured in the yaml file under process_logic
# it will execute each of the logic on this file in the order it was entered in the yaml file


def process_logic(foi, db, df):
    #table = df.current_file_state.table
    #row = df.current_file_state.row
    #assert isinstance(row, db_table.db_table_def.MetaSourceFiles)
    foi.render_runtime_data(df)
    # store result of action you do in this variable

    if foi.table_name_extract is not None:
        table_name_regex = re.compile(foi.table_name_extract)
        # table_name = table_name_regex.match(table_name))
        #print(foi.table_name_extract, df.curr_src_working_file)
        foi.table_name = migrate_utils.static_func.convert_str_snake_case(
            re.search(foi.table_name_extract, df.curr_src_working_file).group(1))

        # print(foi.table_name)
    #row.reprocess = foi.reprocess
    #table.session.commit()

    process_logic = foi.process_logic
    if foi.pre_action is not None:
        logging.info("Executing Pre Load SQL")
        execute_sql(db, foi.pre_action, foi, df)
     

    for logic in process_logic:

        custom_logic = logic['logic']
        logic_name = custom_logic.split('.')[-1]
        fqn_logic = 'py_dbmigration.{}'.format(custom_logic)
        logic_status = LogicState(logic_name, df.current_file_state)
        # dynmaically import the modeul specified in the yaml file
        # this could be faster if we imported this once but for now it stays here
        logging.debug('Importing Module: {}'.format(custom_logic))

        module = __import__('py_dbmigration.custom_logic',
                            fromlist=[logic_name])

        imp = getattr(module, logic_name)

        logging.info('\t->PID:{} :Custom Logic Start: {}'.format(os.getpid(),custom_logic))
        df.set_work_file_status(db, df.meta_source_file_id, custom_logic)

        time_started = datetime.datetime.now()
        # *************************************************************************
        try:
            imp.process(db, foi, df, logic_status)
            logic_status.completed()
        except Exception as e:
            logging.error(
                "PID: {}, Syntax Error running Custom Logic: {}\n{}".format(os.getpid(),fqn_logic,e))
            logic_status.hardfail('{}: {}'.format(imp.__file__, e))
        # *************************************************************************

        time_delta = (datetime.datetime.now() - time_started)
        logging.info("\t\t\tExecution Time: {}sec".format(time_delta))
        logging.debug('\t->Dynamic Module Ended: {}'.format(custom_logic))

        if not logic_status.continue_processing_logic:
            logging.error(
                '\t->PID: {}, Abort Processing for this file Because of Error: {}'.format(os.getpid(),df.curr_src_working_file))

            #df.set_work_file_status(db, df.meta_source_file_id, 'FAILED', custom_logic+'\n'+str(df.load_status_msg or ''))
            break

    # if everything was kosher else file should have been tailed 'FAILED'
    if logic_status.continue_processing_logic:
        if foi.post_action is not None:
            logging.info("Executing Post Load SQL")
            execute_sql(db, foi.post_action, foi, df)
        #row.file_process_state = FileStateEnum.PROCESSED.value
        #df.set_work_file_status(db, df.meta_source_file_id, 'PROCESSED')

    logic_status.file_state.processed()
    purge.process(db, foi, df)
