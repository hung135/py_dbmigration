# import csv, pandas,        sqlalchemy, os
import os
import db_utils
import pandas as pd
import logging

def move_data(sql_string,table_name):
    file_name= writable_path + '_tmp_{0}{1}.csv'.format(table_name,id)
    copy_status=src_db.copy_to_csv(sql_string,file_name, ',')
    load_status=trg_db.import_file_client_side(file_name, table_name, ',')
    if copy_status!=load_status:
        raise Exception("Rows Loaded Do not match nows Dumped!Please Check this file:{0}".format(file_name))
    else:
        os.remove(file_name)
    return load_status

logging.basicConfig(level='DEBUG')

writable_path = os.environ['WORKINGPATH']

            
            
            
            
            
            
            
            
            



#src_db = db_utils.Connection(host='wdcdwl01', database='fred_lpd', dbschema='fred_lpd')
src_db = db_utils.Connection(host=os.environ['PGHOST_INTERNAL'], 
                            port=os.environ['PGPORT_INTERNAL'],
                            database=os.environ['PGDATABASE_INTERNAL'], 
                            dbschema=os.environ['PGDATABASE_INTERNAL'],
                            userid=os.environ['PGUSER_INTERNAL'],
                            password=os.environ['PGPASSWORD_INTERNAL'])

trg_db = db_utils.Connection(host=os.environ['PGHOST_EXTERNAL'], 
                            port=os.environ['PGPORT_EXTERNAL'],
                            database=os.environ['PGDATABASE_EXTERNAL'], 
                            dbschema=os.environ['PGDATABASE_EXTERNAL'],
                            userid=os.environ['PGUSER_EXTERNAL'],
                            password=os.environ['PGPASSWORD_EXTERNAL'])
# trg_db.truncate_table('fred_lpd.loan_performance')
# trg_db.truncate_table('fred_lpd.loan_origination')

delta_query = "select distinct source_id from fann_lpd.loan_acquisition"
data_query1 = "select * from fann_lpd.loan_performance where source_id in ({0}) "
data_query2 = "select * from fann_lpd.loan_acquisition where source_id in ({0}) "
# src_df = pd.DataFrame(src_db.query("select distinct source_id from fann_lpd.loan_acquisition"))
# trg_df = pd.DataFrame(trg_db.query("select distinct source_id from fann_lpd.loan_acquisition"))
src_df = pd.read_sql(delta_query, src_db._conn)
trg_df = pd.read_sql(delta_query, trg_db._conn)


dict = {"src": src_df, "trg": trg_df}

result = pd.concat(dict)
result = result.drop_duplicates('source_id', keep=False)
delta_list = result.source_id.tolist()
# print result.drop_duplicates('source_id', keep=False)
total_records=0

i=0
for id in delta_list:
        #in_string = (','.join(str(source_id) for source_id in delta_list))
    sql_string=data_query1.format(id)
    record_count=move_data(sql_string,'fann_lpd.loan_performance')
    total_records+=record_count
    sql_string=data_query2.format(id)
    record_count=move_data(sql_string,'fann_lpd.loan_acquisition')
    total_records+=record_count

logging.info('TOTAL RECORDS MOVED: {0}'.format(total_records))
