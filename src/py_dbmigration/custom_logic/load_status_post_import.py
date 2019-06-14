

import sys
from py_dbutils.parents import DB as dbconnection
from py_dbmigration.data_file_mgnt.state import FOI, LogicState
import os
import logging
import datetime
import copy

#logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')

'''
  
    Author: Hung Nguyen
    Date created: 6/14/2018
    Python Version: 3.6
    Descripton:
    # takes data and puts it into load status
'''


class LoadStatusTblStruct():
     
    end_date = None
    records_inserted = 0
    current_record_count = None

    def build_sql(self,load_status_id=None):
        sql = 'UPDATE logging.load_status SET {set_values} WHERE id={load_status_id}'
        obj = self
        self_attributes = [a for a in dir(obj) if not a.startswith(
            '__') and not callable(getattr(obj, a))]
        # clean up None
        # for col in self_attributes:
        #     if getattr(self,col) is None:
        #         setattr(self,col,'NULL')
        #sql_columns = ','.join(self_attributes)
        sql_set_values = ','.join(["{}='{}'\n".format(x,str(getattr(obj, x)).replace("'", "''")) if getattr(
            obj, x) is not None else f'{x}=NULL' for x in self_attributes])

        return sql.format(set_values=sql_set_values,load_status_id=load_status_id)

    def __str__(self):
        return self.build_sql()

    def __repr__(self):
        return self.__str__


def custom_logic(db, foi, df, logic_status):
    #going through and pulling values and setting the object above
    curr_row_count=None
    x = LoadStatusTblStruct() 
        
        #zip files don't have direct table mapping to get rows
    if foi.table_name is not None:
        fqn_table_name=f'{foi.schema_name}.{foi.table_name}'
        if foi.table_name is not None:
            try:
                curr_row_count,=db.get_a_row(f'select count(1) from {fqn_table_name}')
                x.current_record_count=curr_row_count
            except Exception as e:
                logging.exception(e)
        x.records_inserted = (int(logic_status.row.total_rows))
        x.previous_record_count = curr_row_count
            
    
    x.success = logic_status.row.file_process_state
    x.end_date = datetime.datetime.now()
    logging.debug(f'updating load_status: id {df.load_status_id}')
    #assuming this gets set in the df object
    db.execute(x.build_sql(df.load_status_id))

    return logic_status


###############################################################################################################################################
# Framework Hook Below
###############################################################################################################################################
def process(db, foi, df, logic_status):
    assert isinstance(foi, FOI)
    assert isinstance(db, dbconnection)
    assert isinstance(logic_status, LogicState)
    return custom_logic(db, foi, df, logic_status)


if __name__ == '__main__':
    x = LoadStatusTblStruct()
    print(x)
