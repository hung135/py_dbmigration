from sqlalchemy import Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base

MetaBase = declarative_base()
DbSchema = 'logging'


class MetaSourceFiles(MetaBase):
    __tablename__ = 'meta_source_files'
    id = Column(Integer, primary_key=True)
    file_name = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    process_date = Column(DateTime)
    embedded_id = Column(String, nullable=True)
    file_type = Column(String, nullable=True)
    file_process_state = Column(String, nullable=True)
    process_start_dtm = Column(DateTime, default=None)
    process_end_dtm = Column(DateTime, default=None)
    current_worker_host = Column(String, nullable=True)
    current_worker_host_pid = Column(Integer, default=0)
    rows_inserted = Column(Integer, default=0)
    data_appended = Column(String, nullable=True)
    file_size = Column(Integer, default=0)
    total_rows = Column(Integer, default=0)
    total_files = Column(Integer, default=0)
    total_files_processed = Column(Integer, default=0)
    last_error_msg = Column(String, nullable=True)
    database_table = Column(String, nullable=True)
    __table_args__ = {"schema": DbSchema}
