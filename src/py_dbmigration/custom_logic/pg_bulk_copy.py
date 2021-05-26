
import os, logging as lg

logging=lg.getLogger() 
import sys
from py_dbutils.rdbms import postgres as db_utils
from py_dbmigration.data_file_mgnt.utils import inject_frame_work_data 
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
from py_dbmigration.data_file_mgnt.data_files import DataFile
import py_dbmigration.db_logging as db_logging
import py_dbmigration.db_table as db_table
import py_dbmigration.migrate_utils.static_func as static_func
 


import pprint


#def bulk_load_dataframe(self, dataframe, table_name_fqn, encoding='utf8', workingpath='MEMORY'):
# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params


# import one file at a time using client side copy command postgres
# standard return will be sucesscode, rows_inserted,description

# def process(db, file, file_id, dbschema):
def custom_logic(db: db_utils.DB, foi: FOI, df: DataFile,logic_status: LogicState):
    
    try:  
         
        table_name = foi.table_name or static_func.convert_str_snake_case(df.curr_src_working_file)
        target_schema = foi.schema_name
        delim = foi.file_delimiter or ','
        use_header = None
        encoding=foi.encoding
        config_column_list = foi.column_list
        
        #override anything passed in at logic level
        if not isinstance(foi.CURRENT_LOGIC_CONFIG,str):
            use_header=foi.CURRENT_LOGIC_CONFIG.get('use_header',use_header)
            target_schema=foi.CURRENT_LOGIC_CONFIG.get('schema_name',target_schema)
            delim=foi.CURRENT_LOGIC_CONFIG.get('delimiter',delim) 
            table_name=foi.CURRENT_LOGIC_CONFIG.get('table_name',table_name)
            encoding=foi.CURRENT_LOGIC_CONFIG.get('encoding',encoding)
            config_column_list=foi.CURRENT_LOGIC_CONFIG.get('column_list',config_column_list)
             
        table_name_fqn = "{}.{}".format(target_schema,table_name) 
        config_column_list = [x.strip() for x in config_column_list.split(',')]  
        db_cols_list =db.get_table_columns(table_name_fqn)
        cols = config_column_list or  db_cols_list
        cols = ','.join(cols)
        with_options=['FORMAT CSV']
        rows_inserted = 0
        data_file = os.path.join(df.source_file_path, df.curr_src_working_file)
        table_exists=db.table_exists(table_name_fqn)
    
        if not table_exists:
            logging.info("\t\tTable Don't exist creating generic table : {}".format(table_name_fqn))
            import pandas 
    
            #sqlalchemy_conn = db.connect_SqlAlchemy()
            csv_reader=pandas.read_csv(data_file, sep=delim, nrows=10,
                                        quotechar='"', encoding=encoding, chunksize=10, 
                                        header=0, index_col=False,
                                        dtype=object)
            
            dataframe=csv_reader.get_chunk(3)
            dataframe.rename(columns=lambda x: str(x).strip(), inplace=True)
            db.create_table_from_dataframe(dataframe,table_name_fqn)
              
        with_options.append(f"ENCODING '{encoding}'")
         
        
            
        #header only works for csv
        if use_header:
            with_options.append("HEADER")
            with open(data_file,'r',encoding=encoding) as f:
                for row in f:
                    cols=row.replace(delim,',')
                    break

        with_options.append(f"DELIMITER '{delim}'")  
            
        ###############THERE EXEC COMMAND LOGIC HERE########################################################
        df.curr_table_row_count=df.get_curr_table_row_count(f'{table_name_fqn}')
        logging.info("\t\tCopy Command STARTED: {0}".format(table_name_fqn))
        
        with_options=','.join(with_options)
        cmd_string = """COPY {table} ({columns}) FROM STDIN WITH ({with_options})""".format(
                table=table_name_fqn, columns=cols,with_options=with_options)
        db.create_cur()
    
        with open(data_file,'r',encoding=encoding) as f:
            db.cursor.copy_expert(cmd_string, f)
            rows_inserted=db.cursor.rowcount
            db.commit()
        logic_status.row.rows_inserted=rows_inserted
        logic_status.row.database_table=table_name_fqn
        
        ###############THERE EXEC COMMAND LOGIC HERE########################################################
        logging.debug("\t\tCommand: {0}".format(cmd_string))
        logging.info("\t\tRows Inserted: {0} ".format(rows_inserted))
        logging.info("\t\tCopy Command Completed: {0}".format(table_name))
    except Exception as e:
        #errors also get printed after returning 
        #logging.error(__file__)
        #logging.error(f"DataFile Path: {data_file}")
        logging.exception(e)
        logic_status.failed(e) 

    return logic_status


def process(db, foi, df,logic_status):
    # variables expected to be populated
    assert isinstance(foi,FOI)
    assert isinstance(db, db_utils.DB)
    assert isinstance(logic_status,LogicState)
    assert isinstance(df,DataFile)
    return custom_logic(db, foi, df,logic_status)