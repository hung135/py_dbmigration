import yaml
import os, logging as lg

logging=lg.getLogger() 
import sys
import pandas as pd
import numpy as np
import xml.etree.ElementTree as etree
 
import py_dbutils.rdbms.postgres as db_utils
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
from py_dbmigration.data_file_mgnt.data_files import DataFile

import re
 
#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')


# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params

# routine to check whether the key already exists in the dictionary
def check_key(dict, key):
  counter = 1
  if key in dict.keys():
    key = key + str((counter + 1))
  else:
    key = key

  return key

# routine to process xml file bases on list of xpath provided
# and return the data in dictionary
def process_xml(file, xpath_list=None):
    if xpath_list is None:
      print("Xpath list is needed for parse the xml file")

    try:
      mytree = etree.parse(file)
      myroot = mytree.getroot()

      # initiate an empty dictionary
      elem_dict={}

      #Loop through xpath list and find child(s)
      for xp in xpath_list:
        for xe in myroot.findall(xp):
          for i in xe:
            #print(xp)
            #print(i.tag, i.text)
            # check to see whether the tag name already exists in the dictionary
            i.tag = check_key(elem_dict, i.tag)
            #print(i.tag)
            elem_dict[i.tag] = i.text
      #print(elem_dict)

    except Exception as e:
            logging.exception(e)

    return elem_dict
  
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
    file_id = df.file_id
    xpath_list = foi.xpath_list
    print("xpath_list")
    print(xpath_list)

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

        if lowercase:
            table_name = str.lower(str(table_name))
        try:

            if limit_rows is not None:
                logging.debug("Pandas Read Limit SET: {0}:ROWS".format(limit_rows))
            foi.table_name = table_name
       
            logging.debug(sys._getframe().f_code.co_name + " : " + file)
                
            elem_dict = process_xml(file, xpath_list)
            dataframe=pd.DataFrame(elem_dict, index=[0])
            print(dataframe)

            if foi.column_list is None:
              foi.column_list=[]
                    
            if not foi.use_header and len(foi.column_list) > 0:
                dataframe.rename(columns=lambda x: str(x).strip(), inplace=True)
                dataframe.columns = map(str,
                                        # foi.column_list
                                        names
                                        )  # dataframe.columns = map(str.lower, dataframe.columns)  # print("----- printing3",dest.column_list, dataframe.columns)
                print('use column_list from yaml')
            else:
              col_list = [str(col).strip() for col in dataframe.columns]
                        
            col_list = dataframe.columns.tolist()
            
            # cols_new = [i.split(' ', 1)[1].replace(" ", "_").lower() for i in col_list]
            cols_new = [migrate_utils.static_func.convert_str_snake_case(i) for i in col_list]
            dataframe.columns = cols_new
            
            logging.debug(
                "\t\tInserting: {0}->{1}-->Chunk#: {2} Chunk Size: {3}".format(foi.schema_name, table_name,
                                                                                    counter, chunk_size))
                ####################################################################################################
            #if counter == 0 and append_file_id:
            if append_file_id:
                dataframe['file_id'] = file_id
            df.curr_table_row_count=df.get_curr_table_row_count(f'{target_schema}.{table_name}')
            dataframe.to_sql(table_name, sqlalchemy_conn, schema=target_schema, if_exists='append',
                                    index=False, index_label=names)
                ####################################################################################################
            if counter == 0:
                    
                rows_inserted = (len(dataframe))
            else:
                rows_inserted = (counter) * chunk_size + (len(dataframe))

            dataframe_columns = dataframe.columns.tolist()
             
            logic_status.row.rows_inserted = rows_inserted
            logic_status.table.session.commit()
            

        except Exception as e:
            logging.exception(e)
            logic_status.failed(e)  
                 
                
                
                
    logging.debug("\t\tRows Inserted: {}".format(rows_inserted))
    
    
    return logic_status

def process(db, foi, df,logic_status):
    # variables expected to be populated
  
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    assert isinstance(df, DataFile)
    return custom_logic(db, foi, df,logic_status)
