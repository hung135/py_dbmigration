import os, logging as lg
import logging.handlers
logging = lg.getLogger()
import pandas as pd

#using root logger so this need to come before any other logger that may get call inside one of the imports below
##############################################
from py_dbutils.rdbms import postgres as db_utils
 
 
def get_db_conn(appname=__file__,loglevel=None):
          
        HOST = os.environ.get('PGHOST','localhost')
        DATABASE = os.environ.get('PGDATABASE','postgres')
        DBPORT= os.environ.get('PGPORT', 5432)
        USERID = os.environ.get('PGUSER', 'docker' )
        DBPASSWORD = os.environ.get('PGPASSWORD','docker')
        DBSCHEMA = os.environ.get('PGDBSCHEMA','postgres')
         
        db = db_utils.DB(host=HOST, userid=USERID, dbname=DATABASE, schema=DBSCHEMA,
                                    pwd=DBPASSWORD,  port=DBPORT, label=os.path.basename(appname),loglevel=loglevel)
        
        return db 
def load_csv(args):
    file=os.path.abspath(args.f)
    if args.t:
        table_name=args.t.lower()
    else:
        table_name=os.path.splitext(os.path.basename(file))[0]
         
    df = pd.read_csv(file, encoding=args.e)
    df.rename(columns=lambda x: x.lower().strip(), inplace=True)
    db=get_db_conn()
    sqlalchemy_conn = db.connect_SqlAlchemy()
    
    if args.truncate:
        print("truncating",table_name)
        db.execute(f'truncate table  "{db.schema}"."{table_name}"')
     
    df.to_sql(table_name, sqlalchemy_conn, schema=db.schema, if_exists='append', index=False)
    cols=','.join(df.columns)
    print('Table Name:',table_name)
    print('Columns:',cols)
    print(len(df)," rows inserted")     
    
def main():

    
    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--f', required=True, help="csv file") 
    parser.add_argument('--ll' , help='set logging mode: debug, info, warn, error ')
    parser.add_argument('--t' , help='table name to load into')
    parser.add_argument('--truncate' , help='truncate the target table',action="store_true")
    parser.add_argument('--d' , help='database connection string')
    parser.add_argument('--c' , help='create table if not already exists',action="store_true")        
    parser.add_argument('--e' , help='encoding',default='utf8')
    parser.add_argument('--v' , help='print version info',action="store_true")
    
    args = parser.parse_args() 
    if args.v:
        from py_dbmigration.version import version
        print(version)
        sys.exit(0) 
    load_csv(args)
if __name__ == '__main__':
    
    
    main()