import yaml
import os
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as dfm
import py_dbmigration.migrate_utils.static_func as static_func

from  py_dbmigration.data_file_mgnt.state import  FOI
import multiprocessing as mp

import pprint


import logging as log
log.basicConfig()
logging = log.getLogger()
logging.setLevel(log.INFO)
logging.setLevel(log.DEBUG)


def merge_two_dicts(x, y):
    # stolen from here
    # https://stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
    z = x.copy()   # start with x's keys and values
    z.update(y)    # modifies z with y's keys and values & returns None
    return z


def process_yaml(yaml_file=None):
    if yaml_file is None:
        yaml_file = os.path.join(os.getcwd(),"data_load.yaml")
    logging.debug("loaind yaml file: {}".format(yaml_file))
    paths = None
     
    with open(yaml_file,'r') as f:
        from yaml import Loader
        paths= yaml.load(f,Loader=Loader)

    datafiles = []
    
    try:
        for path_dict in paths:
            level1={}
            for key in path_dict.keys():
                if key.upper()!='MAPPING':
                    level1[key]=path_dict[key]
        
            for mapping in path_dict['mapping']:
                datafiles.append(FOI(level1,mapping))
 
                                             

    except Exception as e:
        logging.error("Error processing YAML Files: {}\n Error Message: {}".format(yaml_file, e))
        datafiles = []
    return datafiles

def main(yamlfile=None,write_path=None,schema=None,logging_mode=None):


    import sys
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--yaml', help="yaml file")
    parser.add_argument('--cores',default=2, help='Number of Cores(Subprocess) to use')
    parser.add_argument('--logging' , help='set logging mode: debug, info, warning, error ')
    args = parser.parse_args()
    logging_set=args.logging or logging_mode
    if logging_set is 'debug':
 
        logging.setLevel(log.DEBUG)
    else:
 
        logging.setLevel(log.INFO)

    datafiles =None
    if args.yaml is not None:
        datafiles = process_yaml(os.path.abspath(args.yaml))
    else:
        datafiles = process_yaml(yamlfile)
 
    writable_path = write_path or os.getenv('WORKINGPATH',os.getcwd())  
    PGDATASCHEMA = schema or os.getenv('PGDATASCHEMA',schema)
    
    if len(datafiles) > 0:
        db = db_utils.DB(schema=PGDATASCHEMA)
         
        # db.truncate_table("logging", "meta_source_files")

        df = dfm.data_files.DataFile(writable_path, db, datafiles)
        df.init_db()
        df.reset_meta_table(db, 'FAILED', where_clause=" (1=1) ")

        df.do_work(db, cleanup=False,    skip_ifexists=False)
        db.execute('vacuum analyze logging.meta_source_files')
    else:
        logging.info("No configruation Items found...Exiting.")

if __name__ == '__main__':
    main()