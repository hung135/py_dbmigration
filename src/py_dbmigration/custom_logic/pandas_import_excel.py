import os
import logging
import sys
import pandas as pd
from py_dbutils.rdbms.postgres import DB as db_connection
import py_dbmigration.migrate_utils as migrate_utils
from py_dbmigration.data_file_mgnt.state import LogicState, FOI
import re


def custom_logic(db, foi, df, logic_status):
    lowercase = True
    rows_inserted = 0

    file = os.path.join(df.source_file_path, df.curr_src_working_file)

    table_name = foi.table_name
    column_list = foi.column_list
    target_schema = foi.schema_name
    append_file_id = foi.append_file_id

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

            dataframe = pd.read_excel(
                file, encoding='unicode',  index_col=None, header=0)
            # xl = pd.ExcelFile(file)
            # df = xl.parse(1)
            col_list = dataframe.columns.tolist()
            snaked_columns = [
                migrate_utils.static_func.convert_str_snake_case(i) for i in col_list]

            dataframe.columns = snaked_columns

            # df = df[1: 10]
            if append_file_id:
                dataframe['file_id'] = df.meta_source_file_id

            dataframe.to_sql(table_name, sqlalchemy_conn, schema=target_schema, if_exists='append',
                             index=False, index_label=column_list)

            rows_inserted = (len(dataframe))
            logic_status.row.rows_inserted = rows_inserted

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
