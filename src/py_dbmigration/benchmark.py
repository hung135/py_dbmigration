import os, logging as lg
import logging.handlers
logging = lg.getLogger()

#using root logger so this need to come before any other logger that may get call inside one of the imports below
##############################################
import yaml
import psycopg2
import time
import multiprocessing as mp
import sys
import argparse 

 
       

HOST = os.environ.get('PGHOST','localhost')
DATABASE = os.environ.get('PGDATABASE','postgres')
DBPORT= os.environ.get('PGPORT', 5432)
USERID = os.environ.get('PGUSER', 'docker' )
DBPASSWORD = os.environ.get('PGPASSWORD','docker')
DBSCHEMA = os.environ.get('PGDBSCHEMA','postgres')      
         

def run_sql(sql,proc_num):
    
    try:
        connection = psycopg2.connect(user=USERID,
                                    password=DBPASSWORD,
                                    host=HOST,
                                    port=DBPORT,
                                    database=DATABASE,
                                    application_name=f"bench_mark_{proc_num}")
        cursor = connection.cursor()
        postgreSQL_select_Query = sql

        cursor.execute(postgreSQL_select_Query) 
          
        col1, = cursor.fetchone()
        
         
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)

    finally:
        # closing database connection.
        if connection:
            cursor.close()
            connection.close()
            #print(f"Connection {proc_num} is closed")


def process_yaml(yaml_file=None):
    if yaml_file is None:
        yaml_file = os.path.join(os.getcwd(),"benchmark.sql")
    logging.debug("Loading yaml file: {}".format(yaml_file))
     
    
    with open(yaml_file, 'r', encoding='utf-8') as sqlfile: 
        y = yaml.safe_load(sqlfile)
    return y
 
     

def main(yamlfile=None,write_path=None,schema=None,logging_mode=None,cores=None):

    
    datafiles =None
    if yamlfile is None:
 
        parser = argparse.ArgumentParser()
        parser.add_argument('--yaml', help="yaml file")
        parser.add_argument('--cores',default=1, help='Number of Cores(Subprocess) to use')
        parser.add_argument('--ll' , help='set logging mode: debug, info, warn, error ')
        parser.add_argument('--lf' , help='path to loging file')
        parser.add_argument('--v' ,'--version', help='print version info',action="store_true")
        
        args = parser.parse_args() 
    
        if args.v:
            from py_dbmigration.version import version
            print(version)
            sys.exit(0) 
        if str(args.ll).isnumeric():
            logging.setLevel(int( args.ll))
        elif not (args.ll is None):
            logging.setLevel(str( args.ll).upper())
        yamlfile=args.yaml
        
    else:
        if (logging_mode is not None) and str(logging_mode).isnumeric():
             
            logging.setLevel(int( logging_mode))
        elif (logging_mode is not None):
            logging.setLevel(str( logging_mode).upper())

        
    sql_list = process_yaml(yamlfile)
    
    PGDATASCHEMA = schema or os.getenv('PGDATASCHEMA',schema)
    

    sub_proc_count=int(args.cores or 1)
    
    manager = mp.Manager()
    return_dict = manager.dict()
    multi_process(
        mp_do_work, [sql_list], int(sub_proc_count), return_dict)
   
   
    

def mp_do_work(sql_list,proc_num, return_dict):
     
    print(sql_list)
    counter=0
    for sql in sql_list:
        counter+=1
        print(f"Running SQL {counter}, Connection {proc_num}")
        t = time.process_time()
        run_sql(sql,proc_num)
        elapsed_time = time.process_time() - t
        print(f"SQL {counter}, Connection {proc_num}: RUN Time {elapsed_time}")
    return_dict['proc_num{}'.format(proc_num)]='Started'
    #logging.info(">>>>>>>>>>>Proc Started: {}".format(proc_num))
    return_dict['proc_num{}'.format(proc_num)]='Done'
    #logging.info(">>>>>>>>>>>Proc Done: {}".format(proc_num))
   

def multi_process(funct, list_params, max_connections, p_return_dict=None):
    import copy
    process_list = []

    # c = mp.cpu_count()
    # if c > max_cores:
    #     c = max_cores
    c = max_connections
    print(c)
    for proc_num, i in enumerate(range(0, c)):
        list_p = copy.copy(list_params)
        list_p.append(proc_num)
        list_p.append(p_return_dict)
        e = mp.Process(target=funct, args=tuple(list_p))
        process_list.append(e)
        logging.info("Start Connection : {}".format(i))

        e.start()
 
    for i, proc in enumerate(process_list):
        proc.join()
        logging.info("Connection Done: {} PID: {}".format(i, proc.pid))

if __name__ == '__main__':
    
    
    main()