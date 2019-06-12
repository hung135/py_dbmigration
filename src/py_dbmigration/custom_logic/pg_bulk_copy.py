
import logging
import os
import sys
from py_dbutils.rdbms import postgres as db_utils
from py_dbmigration.data_file_mgnt.utils import inject_frame_work_data 
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
import py_dbmigration.migrate_utils.static_func as static_func
 


import pprint

logging.basicConfig(level='DEBUG')
#def bulk_load_dataframe(self, dataframe, table_name_fqn, encoding='utf8', workingpath='MEMORY'):
# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params


# import one file at a time using client side copy command postgres
# standard return will be sucesscode, rows_inserted,description

# def process(db, file, file_id, dbschema):
def custom_logic(db, foi, df,logic_status):
 
     
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
 

    

    rows_inserted = 0
  
    data_file = os.path.join(df.source_file_path, df.curr_src_working_file)
  
    table_name = foi.table_name or static_func.convert_str_snake_case(df.curr_src_working_file)
    
    target_schema = foi.schema_name
    table_name_fqn = "{}.{}".format(target_schema,table_name)
     
    header = foi.header_row
    delim = foi.file_delimiter or ','

    table_exists=db.table_exists(table_name_fqn)
 
    if not table_exists:
        logging.info("\t\tTable Don't exist creating generic table : {}".format(table_name_fqn))
        import pandas 
 
        #sqlalchemy_conn = db.connect_SqlAlchemy()
        csv_reader=pandas.read_csv(data_file, sep=delim, nrows=10,
                                    quotechar='"', encoding=foi.encoding, chunksize=10, 
                                    header=0, index_col=False,
                                    dtype=object)
         
        df=csv_reader.get_chunk(3)
        df.rename(columns=lambda x: str(x).strip(), inplace=True)
        db.create_table_from_dataframe(df,table_name_fqn)
         
      
  
    db_cols_list =db.get_table_columns(target_schema+'.'+table_name)
     
    cols = foi.column_list or  db_cols_list
    encoding = foi.encoding
 
    column_count=len(cols)
     
    #count_column_csv(full_file_path, header_row_location=0, sample_size=200, delimiter=','):
    logging.debug("Delimiter: {}".format(foi.file_delimiter))
    file_column_count=static_func.count_column_csv(data_file,header,10,foi.file_delimiter)

    
    if column_count!=file_column_count:
        logging.info('Using column_list2 since column counts differr:')
        logging.info('Config Column Count:{} Datafile Column Count: {}'.format(column_count,file_column_count))
        cols=foi.column_list2

 
    cols = ','.join(cols)
    
     
    header = ''
    if foi.use_header or foi.has_header:
        header = 'HEADER,'
    #header only works for csv
    if foi.use_header:
        with open(data_file,'r') as f:
            for row in f:
                cols=row.replace(delim,',')
                break
    delim = foi.file_delimiter
    if foi.new_delimiter is not None:
        delim = foi.new_delimiter
     
    # logging.debug("Into Import CopyCommand: {0}".format(dest.schema_name + "." + dest.table_name))
    if db is not None:

 
        ###############THERE EXEC COMMAND LOGIC HERE########################################################
        
        logging.info("\t\tCopy Command STARTED: {0}".format(table_name_fqn))
        cmd_string = """COPY {table} ({columns}) FROM STDIN WITH ({header} FORMAT CSV, ENCODING '{encoding}')""".format(
                table=table_name_fqn, columns=cols,header=header,encoding=encoding)
        db.create_cur()
        try: 
            with open(data_file,'r') as f:
                db.cursor.copy_expert(cmd_string, f)
                rows_inserted=db.cursor.rowcount
                db.commit()
            logic_status.row.rows_inserted=rows_inserted
            logic_status.row.database_table=table_name_fqn
        except Exception as e:
            logging.error(__file__)
            logging.error(e)
            logic_status.row.reprocess=foi.reprocess or False
            logic_status.failed(e) 

            
        ###############THERE EXEC COMMAND LOGIC HERE########################################################
        logging.debug("\t\tCommand: {0}".format(cmd_string))
        logging.info("\t\tRows Inserted: {0} ".format(rows_inserted))
        logging.info("\t\tCopy Command Completed: {0}".format(table_name))
    
 
    return logic_status


def process(db, foi, df,logic_status):
    # variables expected to be populated
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    return custom_logic(db, foi, df,logic_status)