

import sys
from py_dbutils.rdbms.postgres import DB 
from py_dbmigration.data_file_mgnt.state import FOI, LogicState
from py_dbmigration.data_file_mgnt.data_files import DataFile
import os
import logging
import datetime
import copy



'''
  
    Author: Hung Nguyen
    Date created: 6/14/2018
    Python Version: 3.6
    Descripton:
    # takes data and puts it into load status
'''


class LoadStatusTblStruct():
    table_name = None
    program_unit = os.path.basename(__file__)
    program_unit_type_code = "o'hara"
    file_path = None
    created_date = None
    created_by = None
    success = None
    start_date = None
    end_date = None
    previous_record_count = None
    records_inserted = None
    records_deleted = None
    records_updated = None
    current_record_count = None

    def build_sql_insert(self):
        sql = 'INSERT INTO logging.load_status \n({columns}) \nvalues({values})'
        obj = self
        self_attributes = [a for a in dir(obj) if not a.startswith(
            '__') and not callable(getattr(obj, a))]
        # clean up None
        # for col in self_attributes:
        #     if getattr(self,col) is None:
        #         setattr(self,col,'NULL')
        sql_columns = ','.join(self_attributes)
        sql_values = ','.join(["'{}'".format(str(getattr(obj, x)).replace("'", "''")) if getattr(
            obj, x) is not None else 'NULL' for x in self_attributes])

        return sql.format(columns=sql_columns, values=sql_values)

    def __str__(self):
        return self.build_sql_insert()

    def __repr__(self):
        return self.__str__


def custom_logic(db: DB, foi: FOI, df: DataFile,logic_status: LogicState):
    #going through and pulling values and setting the object above
    create_date=datetime.datetime.now()
    x = LoadStatusTblStruct()
    fqn_table_name=f'{foi.schema_name}.{foi.table_name}'
     
    if foi.table_name is not None:
        try:
            curr_row_count,=db.get_a_row(f'select count(1) from {fqn_table_name}')
            x.previous_record_count = int(curr_row_count)
            x.current_record_count = int(curr_row_count)
        except Exception as e:
            logging.exception(e)

    
    x.table_name = foi.table_name
    x.program_unit = os.path.basename(df.full_file_path)
    x.program_unit_type_code = int(df.file_id)
    x.file_path = df.full_file_path
    x.created_date = create_date
    x.created_by = db.userid
    x.success = 0
    x.start_date = datetime.datetime.now()
    x.end_date = None
    x.records_inserted = (int(logic_status.row.rows_inserted))
    x.records_deleted = 0
    x.records_updated = 0
    db.execute(x.build_sql_insert())
    sql=f"""select id from logging.load_status where created_date='{x.created_date}' 
                        and program_unit_type_code='{x.program_unit_type_code}' order by id desc limit 1"""
    #place holder for now
    df.load_status_id,=db.get_a_row(sql)

    return logic_status


###############################################################################################################################################
# Framework Hook Below
###############################################################################################################################################
def process(db, foi, df, logic_status):
    # assert isinstance(foi, FOI)
    # assert isinstance(db, DB)
    # assert isinstance(logic_status, LogicState)
    # assert isinstance(df, DataFile)
    return custom_logic(db, foi, df, logic_status)


if __name__ == '__main__':
    x = LoadStatusTblStruct()
    print(x)
