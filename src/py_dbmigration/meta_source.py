 
import os
import py_dbutils.rdbms.postgres as db_utils
 

import pprint
from prettytable import PrettyTable

import logging as log
log.basicConfig()
logging = log.getLogger()
logging.setLevel(log.INFO) 

def print_table_state(db):
        proj , _=(db.query("""select distinct project_name,file_process_state,count(*) as files 
                        from logging.meta_source_files group by project_name,file_process_state order by 1,2"""))
        t = PrettyTable(['Project', 'Status','File Count'])
        for r in proj:
            t.add_row(r)
        print(t) 
def print_table_file_state(db,project,state):
        where_project='1=1'
        where_state='1=1 and '
        if project!='ALL':
            where_project="upper(project_name) = upper('{}') ".format(project)
        if state!='ALL':
            where_state="upper(file_process_state)=upper('{}') and ".format(state)
        proj , meta=(db.query("""select distinct project_name,file_process_state,file_name ,
                        total_rows as file_rows,rows_inserted,last_error_msg,process_msg_trail
                        from logging.meta_source_files where {} {} order by 3,2""".format(where_state,where_project)))
        cols=[]
        for col in meta:
            cols.append(col.name)
        t = PrettyTable(cols)
        for r in proj:
            t.add_row(r)
        print(t) 
def main():


    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--p', required=True, help='Project Name, ''ALL'' or Project Name')
    parser.add_argument('--r',  help='Reset Project, FAILED, STUCK (or in progress), ALL , CLEAN = Delete records from meta source')
    parser.add_argument('--s',  help='List all files in these states, FAILED, PROCESSED, OBSOLETE, Processing,DUPLICATE')
    db=db_utils.DB()


    args = parser.parse_args()
    print_table_state(db)
    
    
    where_clause_project = """ project_name ='{}' and """.format(args.p)
    
    if args.p == 'ALL':
        where_clause_project=""
    else:
        cnt,=db.get_a_row("select count(*) from logging.meta_source_files where project_name='{}'".format(args.p))
        if int(cnt)==0:
            sys.exit("ERROR! - Project Name was not found!")

 
    
    
    if args.s is not None:
        print_table_file_state(db,args.p,args.s)
    if args.r is not None:
        if args.r in ('FAILED', 'Processing'):
            db.execute("""update logging.meta_source_files
                        set file_process_state='RAW',
                            process_start_dtm=NULL,
                            process_end_dtm=NULL,
                            current_worker_host=NULL,
                            current_worker_host_pid=NULL,
                            reprocess=TRUE
                            where {} file_process_state='{}'
                    """.format(where_clause_project, args.r))
        if args.r in ('STUCK') :
            db.execute("""update logging.meta_source_files
                        set file_process_state='RAW',
                            process_start_dtm=NULL,
                            process_end_dtm=NULL,
                            current_worker_host=NULL,
                            current_worker_host_pid=NULL,
                            reprocess=TRUE
                            where {} (file_process_state not in ('FAILED', 'PROCESSED','OBSOLETE','DUPLICATE'))
                    """.format(where_clause_project))
        if args.r=='CLEAN':
            if args.p=='ALL':
                print('ERROR: For your protection, Clean ALL is not permitted, Clean each project individually')
                sys.exit(1)
            else:
                db.execute("""delete from logging.meta_source_files where {}  1=1""".format(where_clause_project))
                #print("""delete from logging.meta_source_files where {}  1=1""".format(where_clause_project))
        print(args.p,args.r)
        print_table_state(db)

if __name__ == '__main__':
    main()