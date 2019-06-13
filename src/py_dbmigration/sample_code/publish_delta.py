# import csv, pandas,        sqlalchemy, os
 
import py_dbutils.rdbms.postgres as db_utils
import data_file_mgnt as dfm
import migrate_utils as migu
import db_table
import datetime as dt
import pprint
import sys


import pandas as pd
import os, logging as log
logging = log.getLogger(f'PID:{os.getpid()} - {os.path.basename(__file__)}')
logging.setLevel(log.DEBUG)

FILE_PATH = os.environ['RAWFILEPATH']
writable_path = os.environ['WORKINGPATH']
"""
    Creating a Database connection object
"""

# import csv, pandas,        sqlalchemy, os


def move_data(sql_string, table_name):
  file_name = writable_path + '_tmp_{0}{1}.csv'.format(table_name, id)
  copy_status = src_db.copy_to_csv(sql_string, file_name, ',')
  load_status = trg_db.import_file_client_side(file_name, table_name, ',')
  if copy_status != load_status:
    raise Exception(
        "Rows Loaded Do not match nows Dumped!Please Check this file:{0}".format(file_name))
  else:
    os.remove(file_name)
  return load_status


logging.basicConfig(level='DEBUG')

writable_path = os.environ['WORKINGPATH']


src_db = db_utils.DB(host=os.environ['PGHOST_INTERNAL'],
                                    port=os.environ['PGPORT_INTERNAL'],
                                    database=os.environ['PGDATABASE_INTERNAL'],
                                    dbschema=os.environ['PGDATABASE_INTERNAL'],
                                    userid=os.environ['PGUSER_INTERNAL'],
                                    password=os.environ['PGPASSWORD_INTERNAL'])

trg_db = db_utils.DB(host=os.environ['PGHOST_EXTERNAL'],
                                    port=os.environ['PGPORT_EXTERNAL'],
                                    database=os.environ['PGDATABASE_EXTERNAL'],
                                    dbschema=os.environ['PGDATABASE_EXTERNAL'],
                                    userid=os.environ['PGUSER_EXTERNAL'],
                                    password=os.environ['PGPASSWORD_EXTERNAL'])

database_name = os.environ['PGDATABASE_INTERNAL']
file_name=os.path.basename(__file__)
record_keeper = db_table.db_table_func.RecordKeeper(
    trg_db, db_table.db_table_def.PublishLog,appname=file_name)

# row = t.get_record(db_table.MetaSourceFiles.id == self.meta_source_file_id)
# row.process_end_dtm = dt.datetime.now()

table_list = dict()
limit_rows = 1000
# table_list.append('bk_mpo.loanstats_loss_mit')

data_query = "SELECT * from {0} where file_id in ({1}) "


delta_query = """SELECT  distinct a.id as file_id from logging.meta_source_files a where file_type in ('DATA','CSV')  {} """
delta_query_prod = """SELECT distinct cast(data_id as int) as file_id from logging.publish_log 
                        where   row_counts>0 and file_name!='None' {} """


table_list['bk_mpo.loan_month_new'] = {'trg_table': 'bk_mpo.loan_month_new',
                                       'src_delta_sql': delta_query.format(""" and ( database_table ='bk_mpo.loan_month_new' ) """),
                                       'trg_delta_sql': delta_query_prod.format(""" and table_name='bk_mpo.loan_month_new' """)}

table_list['bk_mpo.loan_month_new2'] = {'trg_table': 'bk_mpo.loan_month_new',
                                        'src_delta_sql': delta_query.format(""" and ( database_table ='bk_mpo.loan_month_new2' ) """),
                                        'trg_delta_sql': delta_query_prod.format(""" and table_name='bk_mpo.loan_month_new' """)}

#key, values
for src_table, value in table_list.items():
  trg_table = value['trg_table']
  src_delta_sql = value['src_delta_sql']
  trg_delta_sql = value['trg_delta_sql']

  src_df = pd.read_sql(src_delta_sql, src_db._conn)
  trg_df = pd.read_sql(trg_delta_sql, trg_db._conn)

  s = set(src_df['file_id'])
  t = set(trg_df['file_id'])

  delta_list = s - t

  print("Moving These IDs", delta_list)

  i = 0
  for id in delta_list:
    i += 1
    publish_status = 'Started'
    total_records = 0
    # in_string = (','.join(str(file_id) for file_id in delta_list))
    row = db_table.db_table_def.PublishLog(data_id=id, publish_start_time=dt.datetime.now(),
                                           schema='bk_mpo', table_name=trg_table,
                                           user_name=os.environ['PGUSER_EXTERNAL'], publish_status=publish_status)

    print("Dumping FILE_ID: ", id)
    sql_string = data_query.format(src_table, id)
    try:
      record_keeper.session.commit()
      record_count = move_data(sql_string, trg_table)
      total_records += int(record_count)
      publish_status = 'Completed'
       
      record_keeper.session.commit()
    except Exception as e:
      import datetime

      publish_status = 'Failed'
       
    row.publish_end_time = dt.datetime.now()
    row.row_counts = total_records
    row.publish_status = publish_status
    # query soource databse for file name
    sql_get_file_name = "SELECT file_name, file_path from logging.meta_source_files where id ={}"
    f = src_db.query(sql_get_file_name.format(id))
     
    row.file_name = f[0][0]
    row.file_path = f[0][1]
    record_keeper.add_record(row, commit=True)
    record_keeper.session.commit()
    # record_keeper.commit()

    logging.info('TOTAL RECORDS MOVED: {0}'.format(total_records))


html_css = r"""

    <!DOCTYPE html>
    <html>
    <head>
      <title>Stupid jQuery table sort</title>
      <script src="https://ajax.googleapis.com/ajax/libs/jquery/1.9.1/jquery.min.js"></script>
      <script src="../stupidtable.js?dev"></script>
      <script>
        $(function(){
            $("table").stupidtable();
        });
      </script>
      <style type="text/css">
        table {
          border-collapse: collapse;
        }
        th, td {
          padding: 5px 10px;
          border: 1px solid #999;
        }
        th {
          background-color: #eee;
        }
        th[data-sort]{
          cursor:pointer;
        }
        tr.awesome{
          color: red;
        }
      </style>
      </style>
    </head>

    <body>
      
    """

migu.static_func.make_html_meta_source_files(
    src_db, '/home/dtwork/static/dbmigration/{}/meta_source_files.html'.format(database_name), html_css)
migu.static_func.make_html_publish_log(
    trg_db, '/home/dtwork/static/dbmigration/{}/publish_log.html'.format(database_name), html_css)
migu.static_func.make_html_publish_error(
    trg_db, '/home/dtwork/static/dbmigration/{}/publish_error.html'.format(database_name), html_css)
