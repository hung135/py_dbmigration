import os
import logging
import sys
import pandas as pd
from py_dbutils.rdbms.postgres import DB as db_connection

import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
from py_dbmigration.data_file_mgnt.data_files import DataFile

import re
import py_dbutils.rdbms.postgres as db_utils


# Routine to process excel file
def custom_logic(db: db_utils.DB, foi: FOI, df: DataFile,logic_status: LogicState):
    lowercase = True
    rows_inserted = 0

    file = os.path.join(df.source_file_path, df.curr_src_working_file)

    table_name = foi.table_name
    column_list = foi.column_list
    target_schema = foi.schema_name
    append_file_id = foi.append_file_id
    sheet_list = foi.sheet_list
    header_row_location = foi.header_row

    # add code to handle parse sheet_list field
    if sheet_list is not None:
        sheet_list = sheet_list.replace(', ', ',').replace('\n', '').split(',')
        #print("sheet_list: ", sheet_list)
    else:
        sheet_list = None
        logging.info("must have one sheet listed")
    print(sheet_list)

    if db is not None:

        sqlalchemy_conn = db.connect_SqlAlchemy()

        if table_name is None:
            table_name = str(os.path.basename((file)))

        make_snake_case = foi.convert_table_name_snake_case or False
        if make_snake_case:
            table_name = migrate_utils.static_func.convert_str_snake_case(
                table_name)

        if lowercase:
            table_name = str.lower(str(table_name))
        try:
            foi.table_name = table_name

            logging.info("Reading Excel File")
            print(sheet_list)
            # loop through the sheet(s) that need to process
            for s in sheet_list:
                print("Processing Sheet: ", s)
                print(s)

                # check to for specify columns that need to process
                sheet_col=s.split('|')
                print(sheet_col)
                if len(sheet_col) == 2:
                    sn=sheet_col[0]
                    print(sn)
                    pcol=sheet_col[1]
                    print(pcol)
                else:
                    sn=sheet_col[0]
                    pcol=None
                    print("no col value")
                    print(sn)
                    print(pcol)

                print("foi.header_row_location")
                print(header_row_location)

                # check for sheet_name
                if s is not None:
                    dataframe = pd.read_excel(file, sheet_name=int(args.s), encoding='unicode',  index_col=None, usecols=pcol, header=header_row_location)
                else:
                    dataframe = pd.read_excel(file, encoding='unicode',  index_col=None, usecols=None, header=header_row_location)

                #print("append fileid")
                # Append file_id to dataframe
                if append_file_id:
                    dataframe['file_id'] = df.file_id

                #print("add sheet_nam")
                # Append sheet_name to dataframe
                dataframe['tier_name'] = sn

                print("read dataframe")
                print(dataframe)

                # Check whether list of column name is supplied
                print("foi.column_list")
                print(foi.column_list)
                if foi.column_list is None:
                    foi.column_list=[]
                
                print(foi.use_header)
                print(len(foi.column_list))
                # not use header row and column_list attribute is provided.
                if not foi.use_header and len(foi.column_list) > 0:
                    dataframe.columns = map(str,
                                            column_list
                                            # names
                                            )
                    print("inside use_header")
                    print("dataframe.columns")
                    print(dataframe.columns)
                else:
                    print("no column_list")
                    #col_list = dataframe.columns.tolist()
                    col_list = [str(col).strip() for col in dataframe.columns]

                col_list = dataframe.columns.tolist()

                cols_new = [migrate_utils.static_func.convert_str_snake_case(i) for i in col_list]
                dataframe.columns = cols_new
                dataframe_columns = cols_new

                dataframe.to_sql(table_name, sqlalchemy_conn, schema=target_schema, if_exists='append',
                                index=False, index_label=column_list) #names)
                dataframe_columns = dataframe.columns.tolist()

                rows_inserted = (len(dataframe))
                logic_status.row.rows_inserted = rows_inserted
                logic_status.row.database_table = table_name
                logic_status.table.session.commit()

        except Exception as e:
            logging.exception(e)
            logic_status.failed(e)

    logging.debug("\t\tRows Inserted: {}".format(rows_inserted))

    return logic_status


def process(db, foi, df, logic_status):
    # variables expected to be populated

    assert isinstance(foi, FOI)
    assert isinstance(db, db_connection)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)
