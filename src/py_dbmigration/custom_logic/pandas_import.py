import yaml
import logging
import os
import sys
import pandas as pd
import numpy as np
 
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.data_file_mgnt as data_file_mgnt
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import *

import re
 

logging.basicConfig(level='DEBUG')

# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params


# def import_file(db, foi, lowercase=True,  chunk_size=10000):
def custom_logic(db, foi, df,logic_status):
 
 
    chunk_size = 50000
    lowercase = True
    rows_inserted = 0
     
    
     
    file = os.path.join(df.source_file_path, df.curr_src_working_file)
    limit_rows = (foi.limit_rows)
     
    table_name = foi.table_name
    target_schema = foi.schema_name
    #table_name_extract = foi.table_name_extract
    header = foi.header_row
    names =  foi.column_list
    file_type = foi.file_type
    file_id = df.meta_source_file_id

    delim = foi.new_delimiter or foi.file_delimiter
    append_file_id = foi.append_file_id
 
    if db is not None:
         
        sqlalchemy_conn = db.connect_SqlAlchemy()

         
        if table_name is None:
            table_name = str(os.path.basename((file)))
         
        make_snake_case=foi.convert_table_name_snake_case or False
        if make_snake_case:
            table_name=migrate_utils.static_func.convert_str_snake_case(table_name)
        counter = 0
        # if table_name_extract is not None:
        #     table_name_regex = re.compile(table_name_extract)
        #     # table_name = table_name_regex.match(table_name))
        #     print("----", table_name_extract, table_name)
        #     table_name = re.search(table_name_extract, table_name).group(1)

        #     logging.info("\t\tExtracted tableName from file: {} ".format(table_name))
        if lowercase:
            table_name = str.lower(str(table_name))
        try:

            if limit_rows is not None:
                logging.debug("Pandas Read Limit SET: {0}:ROWS".format(limit_rows))
            foi.table_name = table_name
       

            logging.debug(sys._getframe().f_code.co_name + " : " + file)

          
            if file_type == 'CSV':
                for counter, dataframe in enumerate(
                        pd.read_csv(file, sep=delim, nrows=limit_rows, encoding=foi.encoding, chunksize=chunk_size, 
                                    header=header, index_col=False, dtype=object)):
                    
                    if foi.column_list is None:
                        foi.column_list=[]
                     
                    if not foi.use_header and len(foi.column_list) > 0:
                        dataframe.rename(columns=lambda x: str(x).strip(), inplace=True)
                        dataframe.columns = map(str,
                                                # foi.column_list
                                                names
                                                )  # dataframe.columns = map(str.lower, dataframe.columns)  # print("----- printing3",dest.column_list, dataframe.columns)
                    else:
                         
                         
                        col_list = [str(col).strip() for col in dataframe.columns]
                         

                # cols_new = [i.split(' ', 1)[1].replace(" ", "_").lower() for i in col_list]
                        cols_new = [migrate_utils.static_func.convert_str_snake_case(i) for i in col_list]
                        dataframe.columns = cols_new
                    logging.info(
                        "\t\tInserting: {0}->{1}-->Chunk#: {2} Chunk Size: {3}".format(foi.schema_name, table_name,
                                                                                       counter, chunk_size))
                    ####################################################################################################
                    if counter == 0 and append_file_id:
                        dataframe['file_id'] = file_id
                    dataframe.to_sql(table_name, sqlalchemy_conn, schema=target_schema, if_exists='append',
                                     index=False, index_label=names)
                    ####################################################################################################
                if counter == 0:
                     
                    rows_inserted = (len(dataframe))
                else:
                    rows_inserted = (counter) * chunk_size + (len(dataframe))

                dataframe_columns = dataframe.columns.tolist()
            else:  # assume everything else is Excel for now
                print("Reading Excel File")

                dataframe = pd.read_excel(file, encoding='unicode',  index_col=None, header=0)
                # xl = pd.ExcelFile(file)
                # df = xl.parse(1)
                col_list = dataframe.columns.tolist()

                # cols_new = [i.split(' ', 1)[1].replace(" ", "_").lower() for i in col_list]
                cols_new = [migrate_utils.static_func.convert_str_snake_case(i) for i in col_list]
                # df.columns = df.columns.str.split(' ', 1)
                dataframe.columns = cols_new
                dataframe_columns = cols_new
                # df = df[1: 10]
                if append_file_id:
                    dataframe['file_id'] = df.meta_source_file_id

                dataframe.to_sql(table_name, sqlalchemy_conn, schema=target_schema, if_exists='append',
                                 index=False, index_label=names)
                dataframe_columns = dataframe.columns.tolist()
                 
                rows_inserted = (len(dataframe))
            logic_status.row.rows_inserted = rows_inserted
            logic_status.table.session.commit()
            

        except Exception as e:
            
            logic_status.failed(e)  
                 
                
                
                
    logging.info("\t\tRows Inserted: {}".format(rows_inserted))
    
    
    return logic_status

def process(db, foi, df,logic_status):
    # variables expected to be populated
  
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)
