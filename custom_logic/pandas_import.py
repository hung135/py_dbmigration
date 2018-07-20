import yaml
import logging
import os
import sys
import pandas as pd
import numpy as np
import db_utils
from openpyxl import load_workbook

import pprint

logging.basicConfig(level='DEBUG')

# leveraging pandas libraries to read csv into a dataframe and let pandas
# insert into database
# @migrate_utils.static_func.timer
#@migrate_utils.static_func.dump_params


# def import_file(db, foi, lowercase=True,  chunk_size=10000):
def process(db, foi, df):
    continue_processing = False
    chunk_size = 50000
    lowercase = True
    rows_inserted = 0
    import_status = None
    additional_info = None
    dataframe_columns = ''
    file = os.path.join(df.source_file_path, df.curr_src_working_file)
    limit_rows = foi.limit_rows
    table_name = foi.table_name
    target_schema = foi.schema_name
    header = foi.header_row
    names = foi.header_list_returned or foi.column_list
    file_type = foi.file_type

    delim = foi.new_delimiter or foi.file_delimiter
    append_file_id = foi.append_file_id

    error_msg = None
    if db is not None:
        sqlalchemy_conn, meta = db.connect_sqlalchemy()

        if table_name is None:
            table_name = migrate_utils.static_func.convert_str_snake_case(os.path.basename((file)))
        counter = 0

        if lowercase:
            table_name = str.lower(str(table_name))
        try:

            if limit_rows is not None:
                logging.debug("Pandas Read Limit SET: {0}:ROWS".format(limit_rows))

            # names = ','.join(foi.column_list)
            # names = ','.join(foi.header_list_returned)

            logging.debug(sys._getframe().f_code.co_name + " : " + file)

            if file_type == 'CSV':
                for counter, dataframe in enumerate(
                        pd.read_csv(file, sep=delim, nrows=limit_rows,
                                    quotechar='"', encoding=foi.encoding, chunksize=chunk_size, header=header, index_col=False,
                                    dtype=object)):

                    if not foi.use_header and len(foi.column_list) > 0:

                        dataframe.columns = map(str,
                                                # foi.column_list
                                                names
                                                )  # dataframe.columns = map(str.lower, dataframe.columns)  # print("----- printing3",dest.column_list, dataframe.columns)
                    logging.info(
                        "\t\tInsert Into DB: {0}->{1}-->Records:{2}".format(foi.schema_name, table_name,
                                                                            counter * chunk_size))
                    ####################################################################################################
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

                df = pd.read_excel(file, encoding='unicode',  index_col=None, header=0)
                # xl = pd.ExcelFile(file)
                # df = xl.parse(1)
                col_list = df.columns.tolist()

                # cols_new = [i.split(' ', 1)[1].replace(" ", "_").lower() for i in col_list]
                cols_new = [migrate_utils.static_func.convert_str_snake_case(i) for i in col_list]
                # df.columns = df.columns.str.split(' ', 1)
                df.columns = cols_new
                dataframe_columns = cols_new
                # df = df[1: 10]
                if append_file_id:
                    df['file_id'] = self.meta_source_file_id

                df.to_sql(table_name, sqlalchemy_conn, schema=target_schema, if_exists='append',
                          index=False, index_label=names)
                dataframe_columns = df.columns.tolist()

            continue_processing = True

        except Exception as e:
            import_status = 'FAILED'

            try:

                # cols_tb = db.get_table_columns(str.lower(str(foi.table_name)))
                # delta = diff_list(dataframe_columns, cols_tb)
                # cols = list(delta)
                # if len(cols) > 1:
                #    cols = str(list(delta))
                logging.error("ERROR: \n---->{0}".format(e))
                error_msg = str(e)[:256]

                # additional_info = (','.join(cols) + str(e))[:2000]
                db.commit()
            except Exception as ee:

                # migrate_utils.static_func.profile_csv(file, ',', 0)
                import time
                print("sleeping so you can read:", ee)
                time.sleep(5)

    return continue_processing
