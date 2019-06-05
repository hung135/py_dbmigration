 
import os
import py_dbutils.rdbms.postgres as db_utils
 

import pprint


import logging as log
log.basicConfig()
logging = log.getLogger()
logging.setLevel(log.INFO) 

 
def main():


    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--p',help='Project Name, ALL for All projects')
    parser.add_argument('--r',  help='Reset Project, FAILED, INPROGRESS, ALL , CLEAN = Delete records from meta source')
    parser.add_argument('--l',   action='store_true', help='List Project')
    args = parser.parse_args()
   
    db=db_utils.DB()
    if args.p=='ALL':
        if args.l:
            proj , _=(db.query("""select distinct project_name,file_process_state,count(*) as files 
                        from logging.meta_source_files group by project_name,file_process_state"""))
            print("Before")
            print(proj)
        if args.r=='FAILED':
            db.execute("""update logging.meta_source_files
                        set file_process_state='RAW',
                            process_start_dtm=NULL,
                            process_end_dtm=NULL,
                            current_worker_host=NULL,
                            current_worker_host_pid=NULL,
                            reprocess=TRUE
                            where file_process_state='FAILED'
                    """)
        if args.r=='CLEAN':
            db.execute("""truncate table logging.meta_source_files""")
        proj , _=(db.query("""select distinct project_name,file_process_state,count(*) as files 
                        from logging.meta_source_files group by project_name,file_process_state"""))
    else:
        proj , _=(db.query("""select  project_name,count(*) as files 
                            from logging.meta_source_files where project_name='{}' 
                            group by project_name""".format(args.l)))     
    print("After")
    print(proj)
if __name__ == '__main__':
    main()