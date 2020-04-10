from sqlalchemy.schema import Column, UniqueConstraint
import sqlalchemy.types as c
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base
import logging as lg 
logging=lg.getLogger('RecordKeeper')
 


MetaBase = declarative_base()
class CFPB(object):
    DbSchema = 'logging'

    def __init__(self,schema=DbSchema):
        self.DbSchema=schema
  

class MetaSourceFiles(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'meta_source_files'

    __table_args__ = (UniqueConstraint('file_name', 'file_path', name='uq_uniq_files'), {"schema": DbSchema})
    id = Column(c.Integer, primary_key=True)
    project_name = Column(c.String(256), nullable=False)
    file_name = Column(c.String(256), nullable=False)
    file_path = Column(c.String(256), nullable=False)
 
    file_name_data = Column(c.String(256), nullable=True)
    file_type = Column(c.String(256), nullable=True)
    file_process_state = Column(c.String(256), default='RAW')
  
    process_start_dtm = Column(c.DateTime, default=None)
    process_end_dtm = Column(c.DateTime, default=None)
    current_worker_host = Column(c.String(256), nullable=True)
    current_worker_host_pid = Column(c.Integer, default=0)
    rows_inserted = Column(c.BigInteger, default=0)
    data_appended = Column(c.String(256), nullable=True)
    file_size = Column(c.BigInteger, default=0)
    total_rows = Column(c.BigInteger, default=0)
    total_files = Column(c.Integer, default=0)
    total_files_processed = Column(c.Integer, default=0)
    last_error_msg = Column(c.String(2000), nullable=True)
    database_table = Column(c.String(256), nullable=True)
    parent_file_id = Column(c.Integer, default=0)
    crc = Column(c.String(32), nullable=True)
    upsert_time = Column(c.DateTime)
    publish_time = Column(c.DateTime)
    upsert_function_name = Column(c.String(256), nullable=True)
    reprocess = Column(c.Boolean, default=True)
    process_msg_trail = Column(c.String(2000), nullable=True)
    duplicate_file = Column(c.Boolean,default=False)
# table to store regex for data files to be imported into a table


#small table that will track pid workers to file id
 
class PidWorker(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'pidworker'
    __table_args__ = (UniqueConstraint('file_id', name=f'pidworker_working_id'), {"schema": DbSchema})
    host = Column(c.String(128), primary_key=True)
    pid = Column(c.Integer, primary_key=True)
    file_id = Column(c.Integer, nullable=True)
    last_checkin = Column(c.DateTime, server_default=func.now())
    current_task = Column(c.String(128), nullable=True)
    task_status = Column(c.String(128), nullable=True)
    detail = Column(c.String(2000), nullable=True)
 
            


# class TableFilesRegex(MetaBase):
#     DbSchema = 'logging'
#     __tablename__ = 'table_file_regex'
#     __table_args__ = {"schema": DbSchema}
#     regex = Column(c.String(256), primary_key=True, nullable=False)
#     delimiter = Column(c.String(16), nullable=False)
#     db_schema = Column(c.String(256), nullable=False)
#     table_name = Column(c.String(256), nullable=False)
#     last_update_time = Column(c.DateTime)
#     append_crc = Column(c.Boolean, default=True)
#     append_file_id = Column(c.Boolean, default=True)
#     active = Column(c.Boolean, default=True)


# table to store regex for directories and data files to search inorder to inventory into meta_source_files
# class DirFilesRegex(MetaBase):
#     DbSchema = 'logging'
#     __tablename__ = 'dir_file_regex'
#     __table_args__ = {"schema": DbSchema}
#     regex = Column(c.String(256), primary_key=True, nullable=False)
#     # to be used to extract data from the directory or file name
#     file_path_data_regex = Column(c.String(256), primary_key=True, nullable=False)
#     directory = Column(c.String(512), nullable=False)
#     table_name = Column(c.String(256), nullable=False)
#     last_update_time = Column(c.DateTime)
#     active = Column(c.Boolean, default=True)


class PublishLog(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'publish_log'
    __table_args__ = {"schema": DbSchema}
    id = Column(c.Integer, primary_key=True)
    data_id = Column(c.String(256))
    publish_start_time = Column(c.DateTime)
    publish_end_time = Column(c.DateTime)
    publish_status = Column(c.String(256))
    schema = Column(c.String(256))
    table_name = Column(c.String(256))

    user_name = Column(c.String(256))
    message = Column(c.String(256))
    row_counts = Column(c.BigInteger)
    file_name = Column(c.String(256))
    file_path = Column(c.String(256))
    def __init__(self,table_name=__tablename__,schema=DbSchema):
        self.DbSchema=schema
        self.__tablename__=table_name

class ErrorLog(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'error_log'
    __table_args__ = {"schema": DbSchema}
    id = Column(c.Integer, primary_key=True)
    file_id = Column(c.Integer)
    error_code = Column(c.String(256))
    error_log_id = Column(c.String(256))
    error_message = Column(c.String(256))
    error_timestamp = Column(c.DateTime)
    user_name = Column(c.String(256))
    program_unit = Column(c.String(256))
    sql_statement = Column(c.String(2000))

    def __init__(self,table_name=__tablename__,schema=DbSchema):
        self.DbSchema=schema
        self.__tablename__=table_name

class LoadStatus(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'load_status'
    __table_args__ = {"schema": DbSchema}
    id = Column(c.Integer, primary_key=True)
    table_name = Column(c.String(256))
    program_unit = Column(c.String(256))
    program_unit_type_code = Column(c.String(256))
    file_path = Column(c.String(256))
    created_date = Column(c.String(256))
    created_by = Column(c.String(256))
    success = Column(c.String(256))
    start_date = Column(c.DateTime)
    end_date = Column(c.DateTime)
    previous_record_count = Column(c.BigInteger)
    records_inserted = Column(c.BigInteger)
    records_deleted = Column(c.BigInteger)
    records_updated = Column(c.BigInteger)
    current_record_count = Column(c.BigInteger)

    def __init__(self,table_name=__tablename__,schema=DbSchema):
        self.DbSchema=schema
        self.__tablename__=table_name