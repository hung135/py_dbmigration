import pandas as pd
import logging
from sqlalchemy import create_engine 
import os 
import sys 
import argparse
from datetime import datetime, timedelta
 


PGHOST = os.environ['PGHOST']
PGPORT = os.environ['PGPORT']
PGDATABASE = os.environ['PGDATABASE']
PGUSER = os.environ['PGUSER']
PGPASSWORD = os.environ['PGPASSWORD']
#a dict to make availe for at runtime for sql queries
today = datetime.today()
yesterday = today - timedelta(days=1)
tomorrow = today + timedelta(days=1)
 
runtime_params={'{today}': today.strftime("%Y%m%d"), 
                '{yesterday}': yesterday.strftime("%Y%m%d"),
                '{tomorrow}': tomorrow.strftime("%Y%m%d")
                }

print(runtime_params)
def number_to_date(number):
    date = datetime(year=int(number[0:4]), month=int(number[4:6]), day=int(number[6:8]))
    return date
def get_engine():
    engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(
        PGUSER, PGPASSWORD, PGHOST, PGPORT, PGDATABASE), client_encoding='utf8')

    #print('postgresql://{0}:{1}{2}:{3}/{4}'.format(PGUSER,"*******" , PGHOST, PGPORT, PGDATABASE))
    return engine
 
 
def sql_execute(conn,sql):
    conn.execute(sql)
 
#stupid function to check for 1 record
def check_loaded(conn,sql):
    df=pd.read_sql_query(sql,conn)
    if len(df)>0:
        return True
    return False
def read_yaml(file_path):
    import yaml

    with open(file_path) as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        return yaml.load(file, Loader=yaml.FullLoader)

def apply_runtime_value(sql,runtime_params):
    return_sql=sql
    for key,v in runtime_params.items():        
        return_sql=return_sql.replace(key,v)

    return return_sql
def parse_sql(dbconnection,yaml,runtime_params):
    for key in yaml:
        logging.debug(f"RUNNING : {key}")
        yaml_content=yaml[key]
        check_if_exists = yaml_content.get('check_if_exists',None)
        
        sql_list = yaml_content.get('sqls',None)
        data_already_exists=False
        
        if check_if_exists is not None:
            logging.debug("Checking Data existance")
            
            data_already_exists=check_loaded(dbconnection,apply_runtime_value(check_if_exists,runtime_params))
        
        if not data_already_exists:
            for sql in sql_list:
                runtime_sql=apply_runtime_value(sql,runtime_params)
                logging.debug(f"Running SQL\n****************************\n{runtime_sql}\n****************************")
                #sql_execute(dbconnection,runtime_sql)
        else:
            logging.debug("Data exists aborting")
            #sql_execute(conn,sql)
        

def main():
    
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", 
            help="Increase output (Debug Log Level)", action="store_true")
    parser.add_argument('-f','--file',     
            help=f'Defaults to ./break_out.yaml', default='break_out.yaml')
    parser.add_argument('-s','--start',     
            help=f'Start date ({today.strftime("%Y%m%d")})', default=today.strftime("%Y%m%d"))
    parser.add_argument('-e', '--end',   
            help=f'End date ({today.strftime("%Y%m%d")})',default=today.strftime("%Y%m%d"))
    

    args = parser.parse_args()
     
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
     
    args = parser.parse_args()

    start_time = datetime.now()
    logging.info('start_time %s', start_time)
    

    dbconnection=get_engine() 
    start_date=number_to_date(args.start)
    end_date=number_to_date(args.end)
    
    print(start_date)
    yaml_sql=read_yaml(args.file)
    
    days=pd.date_range(start_date,end_date-timedelta(days=1),freq='d')
    for day in days:
        s = day.strftime("%Y%m%d")
        runtime_params['{day}']=s
        parse_sql(dbconnection,yaml_sql,runtime_params)


 
    end_time = datetime.now()
    elapsed_time = end_time - start_time
    logging.info('end_time %s', end_time)
    logging.info('elapsed_time %s', elapsed_time)
    sys.exit(exit)
if __name__ == "__main__":
    main()