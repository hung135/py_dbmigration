 
 
import time
import logging as log



logging = log.getLogger()
import_type='client_copy'

def execute_sql(db, sql_list):
    for sql in sql_list:
        # print(sql['sql'], "executing sike", type(sql))
        db.execute(sql['sql'])


def import_data_file(foi, db, df=None):
    IMPORT_VIA_PANDAS = 'IMPORT_VIA_PANDAS'
    IMPORT_VIA_CLIENT_CLI = 'IMPORT_VIA_CLIENT_CLI'
    IMPORT_VIA_CUSTOM_FUNC = 'IMPORT_VIA_CUSTOM_FUNC'
    IMPORT_VIA_WYMAN = 'WYMAN'

    status_dict = {}
    status_dict['import_status'] = 'FAILED'
    status_dict['error_msg'] = ''
    status_dict['rows_inserted'] = 0

    import_type = foi.import_method or import_type
    import_type = import_type.upper()



    if foi.pre_action is not None:
        logging.info("Executing Pre Load SQL")
        execute_sql(db, foi.pre_action)


    #dynmaically import the modeul specified in the yaml file
    module = __import__(import_type.lower())
    print(dir(module))
   
    imp = getattr(module,import_type.lower())
    imp.import_file(db, foi) 
    time.sleep(30)

    status_dict['import_status'] = 'success'
    # df.finish_work(db, status_dict=status_dict, vacuum=True)
    if foi.post_action is not None:
        logging.info("Executing Post Load SQL")
        execute_sql(db, foi.post_action)
    #print("------ list import", dir(__import__()))



    return status_dict
