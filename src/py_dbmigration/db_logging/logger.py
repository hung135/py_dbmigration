import os
# import logging
# import datetime
from py_dbutils.rdbms import postgres as db_utils
import datetime
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from pprint import pprint


class ImportLogger:
    # Reflect each database table we need to use, using metadata
    # LoadStatus=None
    # ErrorLog=None

    table_name = "SetMe"
    program_unit = "SetMe"
    program_unit_type_code = "SetMe"
    file_path = "SetMe"
    created_date = datetime.datetime.now()
    created_by = "SetMe"
    success = "1"
    start_date = datetime.datetime.now()
    end_date = datetime.datetime.now()
    previous_record_count = 0
    records_inserted = 0
    records_deleted = 0
    records_updated = 0
    current_record_count = 0

    # error_log columsn
    error_code = None
    error_log_id = None
    error_message = None
    error_timestamp = datetime.datetime.now()
    metadata = None
    prepare = None
    # program_unit=None
    sql_statement = None

    def __init__(self, db):
        self.Base = automap_base()
        self.engine = create_engine(db.connected_uri)
        self.Base.prepare(self.engine, reflect=True, schema='logging')
        self.LoadStatus = self.Base.classes.load_status
        self.ErrorLog = self.Base.classes.error_log
        self.created_by = db.userid
        self.session = Session(self.engine)

    def insert_LoadStatus(self, **Kwargs):
        x = self.LoadStatus()
        self.fill_defaults(x)

        for key, value in Kwargs.items():
            setattr(x, key, value)
        #self.session.add(x)
        #self.session.commit()
        return x

    def insert_ErrorLog(self, **Kwargs):
        x = self.ErrorLog()
        self.fill_defaults(x)

        for key, value in Kwargs.items():
            # Check to make sure we have the Column in the Table before we set
            getattr(x, key)

            if key == "sql_statement":
                value = str(value)[:2000]
            setattr(x, key, value)
        self.session.add(x)
        self.session.commit()
        return x

    def fill_defaults(self, ptable):
        # print(str(type(tbl)))
        if "load_status" in str(type(ptable)):
            ptable.table_name = self.table_name
            ptable.program_unit = self.program_unit
            ptable.program_unit_type_code = self.program_unit_type_code
            ptable.file_path = self.file_path
            ptable.created_date = self.created_date
            ptable.created_by = self.created_by
            ptable.success = self.success
            ptable.start_date = self.start_date
            ptable.end_date = self.end_date
            ptable.previous_record_count = self.previous_record_count
            ptable.records_inserted = self.records_inserted
            ptable.records_deleted = self.records_deleted
            ptable.records_updated = self.records_updated
            ptable.current_record_count = self.current_record_count
        else:
            ptable.error_code = self.error_code
            ptable.error_log_id = self.error_log_id
            ptable.error_message = self.error_message
            ptable.error_timestamp = self.error_timestamp
            ptable.metadata = self.metadata
            ptable.prepare = self.prepare
            ptable.program_unit = self.program_unit
            ptable.sql_statement = self.sql_statement

    def print_column_loadstatus(self):

        for m in self.LoadStatus.__table__.columns:
            print(m, m.type)

    def print_column_errorlog(self):
        for m in self.ErrorLog.__table__.columns:
            print(m, m.type)
            pprint(dir(m))

    def print_tables(self):
        # print(self.engine.table_names)
        for m in self.Base.classes:
            print(m)
            pprint(dir(m))

    def print_records(self, num_of_records):

        result = self.session.query(self.LoadStatus).limit(num_of_records).all()

        for m in result:
            # pprint(dir(m))
            print(m.table_name, m.program_unit, m.program_unit_type_code, m.file_path, m.success, m.start_date, m.end_date,
                  m.previous_record_count, m.current_record_count, m.records_inserted, m.records_updated, m.records_deleted, m.created_by, m.created_date)
