import logging

from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema

from .meta_source_file import MetaSourceFiles, MetaBase, DbSchema


class RecordKeeper():
    db_url = None

    def __init__(self, db):

        self.table = MetaSourceFiles

        engine = create_engine(db.get_conn_url())
        try:
            engine.execute(CreateSchema(DbSchema))
            logging.debug("Creating Database Schema: {}".format(DbSchema))
        except:
            logging.debug("Schema Already Exists No need to create:")
        # create tables
        MetaBase.metadata.create_all(bind=engine)

        # create session
        Session = sessionmaker()
        Session.configure(bind=engine)
        self.session = Session()

        # reflecting whole schema
        self.metadata = MetaData()
        self.metadata.reflect(bind=engine)
        logging.debug(self.metadata.tables)

    def add_record(self, table, commit=False):

        # add row to database

        self.session.add(table)

        if commit:
            try:
                self.commit()

            except Exception as e:
                logging.error(e)
                print(e)
                self.session.rollback()

    def print_row(self, row):
        print(type(row), dir(row))
        for i in row.keys:
            print(i)

    def get_record(self, *row):
        # update row to database
        row = self.session.query(MetaSourceFiles).filter(*row).order_by(MetaSourceFiles.id.desc()).first()

        return row

    def commit(self):
        try:
            self.session.commit()
        except:
            self.session.rollback()

    def __del__(self):
        try:
            self.session.close()
            logging.debug("Closing meta_source_table Session")
        except:
            logging.error("Error Occured Closing meta_source_table Session")
