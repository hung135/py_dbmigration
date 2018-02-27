from sqlalchemy.schema import Column, UniqueConstraint
import sqlalchemy.types as c

from sqlalchemy.ext.declarative import declarative_base


class ProcessStateEnum(c.Enum):
    raw = 1
    processed = 2
    failed = 3


MetaBase = declarative_base()


class MetaSourceFiles(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'meta_source_files'

    __table_args__ = (UniqueConstraint('file_name', 'file_path', name='uq_uniq_files'), {"schema": DbSchema})
    id = Column(c.Integer, primary_key=True)
    file_name = Column(c.String, nullable=False)
    file_path = Column(c.String, nullable=False)
    process_date = Column(c.DateTime)
    file_name_data = Column(c.String, nullable=True)
    file_type = Column(c.String, nullable=True)
    file_process_state = Column(c.String, default='RAW')
    process_start_dtm = Column(c.DateTime, default=None)
    process_end_dtm = Column(c.DateTime, default=None)
    current_worker_host = Column(c.String, nullable=True)
    current_worker_host_pid = Column(c.Integer, default=0)
    rows_inserted = Column(c.BigInteger, default=0)
    data_appended = Column(c.String, nullable=True)
    file_size = Column(c.BigInteger, default=0)
    total_rows = Column(c.BigInteger, default=0)
    total_files = Column(c.Integer, default=0)
    total_files_processed = Column(c.Integer, default=0)
    last_error_msg = Column(c.String, nullable=True)
    database_table = Column(c.String, nullable=True)
    parent_file_id = Column(c.Integer, default=0)


class PublishLog(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'publish_log'
    __table_args__ = {"schema": DbSchema}
    id = Column(c.Integer, primary_key=True)
    data_id = Column(c.Integer)
    publish_start_time = Column(c.DateTime)
    publish_end_time = Column(c.DateTime)
    publish_status = Column(c.String)
    schema = Column(c.String)
    table_name = Column(c.String)

    user_name = Column(c.String)
    message = Column(c.String)
    row_counts = Column(c.BigInteger)
    file_name = Column(c.String)
    file_path = Column(c.String)


class ErrorLog(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'error_log'
    __table_args__ = {"schema": DbSchema}
    id = Column(c.Integer, primary_key=True)

    error_code = Column(c.String)
    error_log_id = Column(c.String)
    error_message = Column(c.String)
    error_timestamp = Column(c.DateTime)
    user_name = Column(c.String)
    program_unit = Column(c.String)
    sql_statement = Column(c.String)


class LoadStatus(MetaBase):
    DbSchema = 'logging'
    __tablename__ = 'load_status'
    __table_args__ = {"schema": DbSchema}
    id = Column(c.Integer, primary_key=True)
    table_name = Column(c.String)
    program_unit = Column(c.String)
    program_unit_type_code = Column(c.String)
    file_path = Column(c.String)
    created_date = Column(c.String)
    created_by = Column(c.String)
    success = Column(c.String)
    start_date = Column(c.DateTime)
    end_date = Column(c.DateTime)
    previous_record_count = Column(c.BigInteger)
    records_inserted = Column(c.BigInteger)
    records_deleted = Column(c.BigInteger)
    records_updated = Column(c.BigInteger)
    current_record_count = Column(c.BigInteger)
