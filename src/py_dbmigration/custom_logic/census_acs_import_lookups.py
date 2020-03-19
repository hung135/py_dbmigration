import yaml
import os, logging as lg as lg
  
import pandas   
import hashlib
import py_dbutils.rdbms.postgres as db_utils
from  py_dbmigration.utils.func import Func 
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
from py_dbmigration.data_file_mgnt.data_files import DataFile

import re
 
lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.DEBUG)


base_columns = [u'file_id', u'table_id', u'seq', u'line_number',
                u'start_position', u'total_cells_in_table', u'total_cells_in_sequence',
                u'table_title', u'subject_area']
ff = Func()

def get_seq(file, start=0, end=1000, encoding='ISO-8859-1'):

    # db = dbconn.Connection(dbtype='POSTGRES', dbschema='logging', commit=True)
    df = pandas.read_csv(file, encoding=encoding)

    df.columns = [u'file_id', u'table_id', u'seq', u'line_number',
                  u'start_position', u'total_cells_in_table', u'total_cells_in_sequence',
                  u'table_title', u'subject_area']
    df['tbl_uuid'] = df['table_id'] + df['line_number'].apply(lambda x: str(x).zfill(3))
    df['tbl_uuid'] = df['tbl_uuid'].apply(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())

    df_seq = df.seq.unique()
    print(type(df_seq))
    seq = []
    for i in df_seq:
        if int(start) <= i <= int(end):
            seq.append(int(i))
    return seq, df[(df['seq'] >= int(start)) & (df['seq'] <= int(end))]



# def import_file(db, foi, lowercase=True,  chunk_size=10000):
def custom_logic(db, foi, df:DataFile,logic_status:LogicState):
  
    full_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)
      
    table_name = foi.table_name
    target_schema = foi.schema_name
    #table_name_extract = foi

    file_id = df.file_id

    _, df = get_seq(full_file_path) 
     

    year = ff.file_path_extract(logging,full_file_path,r'(\d\d\d\d)\dyr')
    year_type = ff.file_path_extract(logging,full_file_path,r'\d\d\d\d(\d)yr') 
     
    dataframe = pandas.read_csv(full_file_path, encoding='ISO-8859-1')
    db.create_table_from_dataframe(df, '{}.{}'.format(target_schema, table_name))
      
    try:
        dataframe.columns = base_columns
        dataframe['year'] = year
        dataframe['year_type'] = year_type
        dataframe['file_id'] = file_id
        dataframe['seq'] = dataframe['seq'].apply(lambda x: str(x).zfill(3))
        dataframe['tbl'] = dataframe['table_id'] + dataframe['seq']

        db.bulk_load_dataframe(dataframe, f'{target_schema}.{table_name}')
        db.commit()
    except Exception as e:
        logic_status.error_msg(e)
        logic_status.failed("Summary File HAS to be correct.")
        
    return logic_status

def process(db, foi, df,logic_status):
    # variables expected to be populated
  
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    assert isinstance(df, DataFile)
    return custom_logic(db, foi, df,logic_status)
