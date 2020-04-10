import os, logging as lg
import logging.handlers
logging = lg.getLogger()

#using root logger so this need to come before any other logger that may get call inside one of the imports below
##############################################
import yaml
 
from py_dbutils.rdbms import postgres as db_utils
import py_dbmigration.data_file_mgnt as dfm
import py_dbmigration.migrate_utils.static_func as static_func
from  py_dbmigration.data_file_mgnt.utils import  pre_process_yaml as ppy
from  py_dbmigration.data_file_mgnt.state import  FOI
from py_dbmigration.db_table.pid_worker import PidManager
import multiprocessing as mp

import pprint 
import py_dbmigration.custom_logic
import sys 




def merge_two_dicts(x, y):
    # stolen from here
    # https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def process_yaml(yaml_file=None):
    if yaml_file is None:
        yaml_file = os.path.join(os.getcwd(),"data_load.yaml")
    logging.debug("Loading yaml file: {}".format(yaml_file))
    # paths = None
     
    # with open(yaml_file,'r') as f:
    #     from yaml import Loader
    #     paths= yaml.load(f,Loader=Loader)
    paths=ppy(yaml_file)
    datafiles = []
    #print(paths)
     
    
    try:
        for path_dict in paths:
            level1={}
            for key in path_dict.keys():
                
                if key.upper()!='MAPPING':
                    level1[key]=path_dict[key]
            mapping=path_dict.get('mapping',False)
            if mapping==False:
                datafiles.append(FOI(level1))
            else:
                for mapping in path_dict['mapping']:
                    datafiles.append(FOI(level1,mapping))
 
                                             

    except Exception as e:
        logging.exception("Error processing YAML Files: {}\n Error Message: {}".format(yaml_file, e))
        datafiles = []
    return datafiles

#configure the root logger
def configure_logging(loglevel=None,logfile=None):
    
    
    log_file= logfile or os.environ.get('LOGFILE',None)
    log_file_write_mode= os.environ.get('LOGWRITEMODE','w')
    
    #print(os.environ.get("LOGLEVEL", "INFO"))

    log_level=str(loglevel or os.environ.get("LOGLEVEL", "INFO")).upper()
    #print("----logggg",log_level)
    LOGFORMAT=f'%(asctime)s, %(process)d, %(levelname)s,%(filename)s," \t%(message)s"'

    #logging.basicConfig(name=__name__,level=log_level, format=LOGFORMAT)
    if log_file is not None:
        handler = logging.handlers.WatchedFileHandler(
            os.environ.get("LOGFILE", "dataload_log.txt"),log_file_write_mode)
        formatter = logging.Formatter(LOGFORMAT)
        handler.setFormatter(formatter)
        handler.setLevel(log_level)
        
        root = lg.getLogger()
        root.setLevel(log_level)
        root.addHandler(handler)

def main(yamlfile=None,write_path=None,schema=None,logging_mode=None,cores=None):

    

    datafiles =None
    if yamlfile is None:

        import sys
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--yaml', help="yaml file")
        parser.add_argument('--cores',default=1, help='Number of Cores(Subprocess) to use')
        parser.add_argument('--ll' , help='set logging mode: debug, info, warn, error ')
        parser.add_argument('--lf' , help='path to loging file')
        parser.add_argument('--v' , help='print version info',action="store_true")
        
        args = parser.parse_args() 
        if args.v:
            from py_dbmigration.version import version
            print(version)
            sys.exit(0)
        #configure_logging( args.ll or logging_mode,args.lf )
        if str(args.ll).isnumeric():
            logging.setLevel(int( args.ll))
        elif not (args.ll is None):
            logging.setLevel(str( args.ll).upper())
        yamlfile=args.yaml
        cores = int(cores or args.cores)
    else:
        if (logging_mode is not None) and str(logging_mode).isnumeric():
             
            logging.setLevel(int( logging_mode))
        elif (logging_mode is not None):
            logging.setLevel(str( logging_mode).upper())

        
    datafiles = process_yaml(yamlfile)
 
    writable_path = write_path or os.getenv('WORKINGPATH',None)  
    PGDATASCHEMA = schema or os.getenv('PGDATASCHEMA',schema)
    

    sub_proc_count=int(cores or 1)
    if len(datafiles) > 0:

        #so pyinstall will pick it upt
        db = db_utils.DB(schema=PGDATASCHEMA,label='data_load_main',loglevel=logging.level)
        df = dfm.data_files.DataFile(writable_path, db, datafiles)
        df.init_db()
        df.reset_meta_table(db, 'FAILED', where_clause=" (1=1) ")
        
        if sub_proc_count==1:
            pidManager=PidManager(db,'dataload','logging','pidworker',False)
            df.do_work(db, cleanup=False,    skip_ifexists=False,pid=pidManager)
            #db.execute('vacuum analyze logging.meta_source_files')
            pidManager.checkin('EXITING','DONE','Process Ran to Completion')
    

        else:
            # variables visible to all spawn processes
            manager = mp.Manager()
            return_dict = manager.dict()
            multi_process(
                mp_do_work, [datafiles, PGDATASCHEMA,writable_path], int(sub_proc_count), return_dict)
    else:
        logging.info("No configruation Items found...Exiting.")
   
    

def mp_do_work(foi_list, data_schema, writable_path,proc_num, return_dict):
    
    db = db_utils.DB(schema=data_schema,label='mp_do_work'+str(proc_num))
    pidManager=PidManager(db,'dataload','logging','pidworker',False)
    df = dfm.data_files.DataFile(writable_path, db, foi_list)
    
    
    return_dict['proc_num{}'.format(proc_num)]='Started'
    print(">>>>>>>>>>>Proc Started: {}".format(proc_num))
    df.do_work(db, cleanup=False,    skip_ifexists=False,pid=pidManager)
    return_dict['proc_num{}'.format(proc_num)]='Done'
    print(">>>>>>>>>>>Proc Done: {}".format(proc_num))
    pidManager.checkin('EXITING','DONE','Process Ran to Completion')

def multi_process(funct, list_params, max_cores, p_return_dict=None):
    import copy
    process_list = []

    c = mp.cpu_count()
    if c > max_cores:
        c = max_cores
    for proc_num, i in enumerate(range(0, c)):
        list_p = copy.copy(list_params)
        list_p.append(proc_num)
        list_p.append(p_return_dict)
        e = mp.Process(target=funct, args=tuple(list_p))
        process_list.append(e)
        logging.info("Start Process : {}".format(i))

        e.start()

    # p.join()
    # iterate over each process to complete
    for i, proc in enumerate(process_list):
        proc.join()
        logging.info("Process Done: {} PID: {}".format(i, proc.pid))

if __name__ == '__main__':
    
    
    main()