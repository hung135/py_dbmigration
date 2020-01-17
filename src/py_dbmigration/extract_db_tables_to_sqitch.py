#!python

# import csv, pandas,        sqlalchemy, os
 
import argparse
import py_dbutils.rdbms.postgres as db_utils
import sys

import py_dbmigration.migrate_utils.static_func as static_func

import os, logging






pghost = os.environ['PGHOST']
pgdatabase = os.environ['PGDATABASE']
"""
    Creating a Database connection object
"""
def parse_cli_args():
    parser = argparse.ArgumentParser()
 
    parser.add_argument('--o',required=True,help='output_directory')
    parser.add_argument('--s',required=True,help='database schema name')
    parser.add_argument('--ll', default='INFO', help='Logging Default=INFO [DEBUG,INFO,WARN,ERROR]')
    args = parser.parse_args()
    return args
def main(o=None,s=None):
    
    args=None 
    output_directory=None
    schemas=[]
    
    
    if o or s:
        output_directory=o  
        schemas.append(s) 
    else:
        args=parse_cli_args()
        output_directory=os.path.abspath(args.o)
        schemas=[args.s]
    # sqlserver = db_utils.Connection(dbschema='dbo', database='enforce', dbtype='MSSQL', host=host, commit=False)
    db_postgres = db_utils.DB(schema=logging, dbname=pgdatabase,
                                        host=pghost)

    #schemas = get_schema_except(db_postgres, ['op_dba', 'public', 'pg_catalog', 'information_schema', 'citus', 'sys', 'sqitch'])
    for s in schemas:
         
        db_postgres = db_utils.DB(schema=s, dbname=pgdatabase, host=pghost)
        #migu.change_table_owner(db_postgres, s, 'operational_dba')
        #migu.change_view_owner(db_postgres, s, 'operational_dba')

        static_func.print_create_table(db_postgres, folder=output_directory,
                                targetschema=s, file_prefix='{}.'.format(s))
        static_func.print_create_functions(db_postgres, folder=output_directory,
                                    targetschema=s, file_prefix='{}.'.format(s))
        static_func.print_create_views(db_postgres, folder=output_directory,
                                targetschema=s, file_prefix='{}.'.format(s))

if __name__ == '__main__':
    main()