import yaml
import os, logging as lg

 
import sys
import pandas  
import numpy 
 
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
from py_dbmigration.data_file_mgnt.data_files import DataFile
from  py_dbmigration.utils.func import Func 
import re
 

lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.DEBUG)

melt_columns = ['year', 'year_type', 'state', 'seq', 'logrecno']
drop_columns = ["acssf",  'chariter']
base_columns = ["acssf", 'year_type', 'state', 'chariter', 'seq', 'logrecno']
pivot_columns = [u'year', u'year_type', u'state', u'seq', u'logrecno', u'tbl']
base_col_types = [numpy.str, numpy.str, numpy.str, numpy.int32, numpy.int32, numpy.int32]
ff = Func()




def get_seq_columns_names(df, seq):
    import hashlib
    cols = []
    print("Getting Sequence: ", seq)
    dd = df[df.seq == int(seq)]
    dff = dd[(dd.line_number >= int('1'))]

    for index, row in dff.iterrows():
        if (float(row.line_number) - float(int(row.line_number))) == 0:
            # print(row.line_number)
            cols.append(row.table_id + str(int(row.line_number)).zfill(3))

    return cols


# def import_file(db, foi, lowercase=True,  chunk_size=10000):
def custom_logic(db, foi, df,logic_status):
 
 
    chunk_size = 50000
    lowercase = True
    rows_inserted = 0
     
    
     
    full_file_path = os.path.join(df.source_file_path, df.curr_src_working_file)
    full_file_path_moe = "how do we get this"
    
    table_name = foi.table_name
    target_schema = foi.schema_name
    #table_name_extract = foi.table_name_extract
    header = foi.header_row
    names =  foi.column_list
    file_type = foi.file_type
    file_id = df.file_id

    ############################################################################################################################
  

#def parse_acs(   db,  df_lookup, df_work, year, year_type):
 
    file_list_count = []


    dff_buffer = []
    dff_buffer_moe = []
    i = 0
    last = len(df_work)
    running_total = 0
     

    buffer_list_len = 0
    # for file, file_moe in zip(file_list, file_list_moe):
    
    df_lookup='ggetlookfromdb.(year,year_type)'
    year = ff.file_path_extract(logging,full_file_path,r'(\d\d\d\d)\dyr',1)
    year_type = ff.file_path_extract(logging,full_file_path,r'\d\d\d\d(\d)yr',1) 
    sequence_number = ff.file_path_extract(logging,full_file_path,r'(\d\d\d\d\d)\.',-1) 
    data_cols = get_seq_columns_names(df_lookup, sequence_number)
    col_count = len(data_cols)


    dff = pandas.read_csv(full_file_path, header=None, low_memory=False,
                            # names=base_columns + data_cols,
                            dtype=object, engine='c')

    dff_moe = pandas.read_csv(full_file_path_moe, header=None, low_memory=False,
                                # names=base_columns + data_cols,
                                dtype=object, engine='c')
        

    if (len(base_columns + col_count) != len(dff.columns)):
        print(base_columns)
        print(data_cols)
        x = list(dff.columns)
        print(x)
 
        logging.error(f"Data file Columns DONT't line up with Sequence Lookup?: \n\t{full_file_path}")
        logic_status.failed("Data columns don't line up")
    else:
        dff.columns = base_columns + data_cols
        dff_moe.columns = base_columns + data_cols
        dff['year_type'] = year_type  # dff['year_type'].apply(lambda x: utils.split_it(x))
        dff_moe['year_type'] = year_type  # dff_moe['year_type'].apply(lambda x: utils.split_it(x))
        dff['year'] = year
        dff_moe['year'] = year
        dff.drop(drop_columns, axis=1, inplace=True)
        dff_moe.drop(drop_columns, axis=1, inplace=True)
 
        org_size = len(dff)
          
        dff_buffer.append(dff)
        dff_buffer_moe.append(dff_moe)
 
        if buffer_list_len == 1:
            dff_buffer = dff_buffer[0]
            dff_buffer_moe = dff_buffer_moe[0]
        else:
            dff_buffer = pandas.concat(dff_buffer)
            dff_buffer_moe = pandas.concat(dff_buffer_moe)
         
        ee = pandas.melt(dff_buffer, id_vars=melt_columns, var_name='tbl', value_name='stat')
        ee_moe = pandas.melt(dff_buffer_moe, id_vars=melt_columns, var_name='tbl', value_name='moe')
        ee_merged = pandas.merge(ee, ee_moe,
                                    left_on=pivot_columns,
                                    right_on=pivot_columns
                                    )

        dff_buffer = []
        dff_buffer_moe = []
        org_size = len(ee_merged)
        
        ee_merged = ee_merged[(ee_merged['stat'].notna() & ee_merged['moe'].notna())]
        curr_size = len(ee_merged)
        nan_dropped = org_size - curr_size
        if nan_dropped>0:
            logging.warning(f"NaN numbers Dropped: {nan_dropped}")
  
        rows_inserted = db.bulk_load_dataframe(ee_merged, table_name_fqn=f"{target_schema}.{table_name}")
        # ee_merged.dropna(thresh=3)
        logging.info(f"Rows Loaded: {rows_inserted}")
        db.execute("commit")
         
    return logic_status

def process(db, foi, df,logic_status):
    # variables expected to be populated
  
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    assert isinstance(df, DataFile)
    return custom_logic(db, foi, df,logic_status)
