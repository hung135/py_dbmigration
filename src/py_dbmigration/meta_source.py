 

import py_dbutils.rdbms.postgres as db_utils
 

import pprint
from prettytable import PrettyTable

import os, logging


 

def print_table_state(db):
        proj , _=(db.query("""select distinct project_name,file_process_state,count(*) as files 
                        from logging.meta_source_files group by project_name,file_process_state order by 1,2"""))
        t = PrettyTable(['Project', 'Status','File Count'])
        for r in proj:
            t.add_row(r)
        print(t) 
def print_table_file_state(db,project,state,add_fields=''):
        where_project='1=1'
        where_state='1=1 and '
        if project!='ALL':
            where_project="upper(project_name) = upper('{}') ".format(project)
        if state!='ALL':
            where_state="upper(file_process_state)=upper('{}') and ".format(state)
        proj , meta=(db.query("""select distinct project_name as PRJ,concat(id,' : ',file_process_state) as State,file_name ,
                        total_rows as file_rows,rows_inserted as rowInserted,last_error_msg
                        --,process_msg_trail
                        ,concat(file_path,'/',file_name)  as path
                        {}
                        from logging.meta_source_files where {} {} order by 3,2
                        """.format(add_fields,where_state,where_project)))
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
    parser.add_argument('--reset',  help='Reset Project, CHILDREN, FAILED, STUCK (or in progress), ALL , CLEAN = Delete records from meta source')
    parser.add_argument('--s',  help='List all files in these states, FAILED, PROCESSED, OBSOLETE, Processing,DUPLICATE')
    parser.add_argument('--f', default='', help='addition fields from meta_source to display')
    
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
    add_fields=''
    if args.f!='':
        add_fields=","+args.f
    
    if args.s is not None:
        print_table_file_state(db,args.p,args.s,add_fields)
    if args.reset is not None:
        if args.reset in ('FAILED', 'Processing'):
            db.execute("""update logging.meta_source_files
                        set file_process_state='RAW',
                            process_start_dtm=NULL,
                            process_end_dtm=NULL,
                            current_worker_host=NULL,
                            current_worker_host_pid=NULL,
                            reprocess=TRUE
                            where {} file_process_state='{}'
                    """.format(where_clause_project, args.reset))
        elif args.reset in ('STUCK') :
            db.execute("""update logging.meta_source_files
                        set file_process_state='RAW',
                            process_start_dtm=NULL,
                            process_end_dtm=NULL,
                            current_worker_host=NULL,
                            current_worker_host_pid=NULL,
                            reprocess=TRUE
                            where {} (file_process_state not in ('FAILED', 'PROCESSED','OBSOLETE','DUPLICATE'))
                    """.format(where_clause_project))

        elif args.reset=='CLEAN':
            if args.p=='ALL':
                print('ERROR: For your protection, Clean ALL is not permitted, Clean each project individually')
                sys.exit(1)
            else:
                db.execute("""delete from logging.meta_source_files where {}  1=1""".format(where_clause_project))
                #print("""delete from logging.meta_source_files where {}  1=1""".format(where_clause_project))
        elif args.reset=='CHILDREN':
            if args.p=='ALL':
                print('ERROR: For your protection, Clean ALL is not permitted, Clean each project individually')
                sys.exit(1)
            else:
                db.execute("""delete from logging.meta_source_files where {}  1=1 and parent_file_id>0""".format(where_clause_project))
                #print("""delete from logging.meta_source_files where {}  1=1""".format(where_clause_project))
        else: 
            db.execute(F"""update logging.meta_source_files
                        set file_process_state='RAW',
                            process_start_dtm=NULL,
                            process_end_dtm=NULL,
                            current_worker_host=NULL,
                            current_worker_host_pid=NULL,
                            reprocess=TRUE
                            where {where_clause_project} (file_process_state ='{args.reset}')
                    """ )
   
       

if __name__ == '__main__':
    main()