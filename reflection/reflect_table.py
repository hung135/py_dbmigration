import os
# import logging
# import datetime
from .. import db_utils
import datetime as dt
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from pprint import pprint


class MetaSourceFiles:
    # Reflect each database table we need to use, using metadata
    # LoadStatus=None
    # ErrorLog=None


 
    file_name ="SetMe"
    file_path ="SetMe"
    embedded_id ="SetMe"
    file_type ="SetMe"
    file_process_state  ="SetMe"
    rows_inserted = 0,
    data_appended =False
    last_error_msg = "setme"
 


    def __init__(self, db):
        self.Base = automap_base()
        self.engine = create_engine(db.get_conn_url())
        self.Base.prepare(self.engine, reflect=True, schema=db.dbschema)
        self.meta_source_files = self.Base.classes.meta_source_files

        self.session = Session(self.engine)
        self.db=db

    def insert_meta_source_files(self, **Kwargs):
        x = self.meta_source_files()
        #self.fill_defaults(x)
        for key, value in Kwargs.items():
            setattr(x, key, value)
        self.session.add(x)
        self.session.commit()

        return x
    def claim_work(self):
        pass

    def get_row_meta_source_files(self, *args):
 
        return self.session.query(self.meta_source_files).filter_by(*args).all()

    def fill_defaults(self, ptable):
        # print(str(type(tbl)))
        #if "load_status" in str(type(ptable)):
        ptable.file_process_state = self.file_process_state
        ptable.last_error_msg = self.last_error_msg
        ptable.file_path = self.file_name
 

    def print_tables(self):
        # print(self.engine.table_names)
        for m in self.Base.classes:
            print(m)
            pprint(dir(m))
