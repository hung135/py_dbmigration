

 
import hashlib
import py_dbutils.parents as db_utils
from py_dbmigration.data_file_mgnt.data_files import DataFile 
from py_dbmigration import migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI, LogicState
import os, logging as lg

logging=lg.getLogger()


''' 
    Author: Hung Nguyen
    Date created: 7/17/2018
    Date last modified: 7/17/2018
    Python Version: 2.7
    Descripton:
    #  this function will add a crc column to the table if not existing and will generate a checksum based on columns passed in
    #  else it will pull list of columns from db and generate checksum for those columns and set the crc
'''

# sql used to update logging....faster than any framework wrapper
update_sql = """UPDATE logging.meta_source_files set  crc='{}'  where id = {}"""

def fetch_cached_crc(file_path_hash):
    return global_hash.get(file_path_hash,None)
def custom_logic(db: db_utils.DB, foi: FOI, df: DataFile,logic_status: LogicState):
    # def custom_logic(db, schema, table_name, column_list=None, where_clause='1=1'):
    logic_status.table.session.commit()
    abs_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)

    if logic_status.row.crc is not None:
        logging.info("\t\tChecksum already Exists, Skipping:")
    else:

        crc=None
        if logic_status.logic_options.get('use_cache',False):
            crc=fetch_cache_crc(hashlib.md5(abs_file_path))
            logging.info("\t\tFile Not in Known Hash, generating MD5:")
        if crc is None:
        
            crc = migrate_utils.static_func.md5_file_36(abs_file_path)
        logging.debug("\t\t\tMD5: {}".format(crc))
        logic_status.row.crc = crc
        

    return logic_status
# Generic code...put your custom logic above to leave room for logging activities and error handling here if any


def process(db, foi, df,logic_status):
 
     
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
