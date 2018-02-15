import logging
import pprint
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateSchema
import sqlalchemy

from .meta_source_file import MetaSourceFiles, MetaBase, LoadStatus, ErrorLog,PublishLog


class RecordKeeper():
    db_url = None
    table = None
    def __init__(self, db,table_def=None,DbSchema_overide=None ):
        

        if table_def is None:
            self.table = MetaSourceFiles
        else:
            self.table = table_def

        if DbSchema_overide is not None:
            #pprint.pprint((self.table.__dict__))
            self.table.DbSchema=DbSchema_overide
            self.table.__table_args__['schema']=self.table.DbSchema
            #self.table.__table__.Table.schema=self.table.DbSchema
            #self.table.__table_args__.schema=self.table.DbSchema
        #pprint.pprint((self.table.__table__))
        #print ("--tb---",self.table.DbSchema)
        #pprint.pprint((self.table.__dict__))
        #print("----",type(self.table.__table__))
        #print("----",'\n',type(self.table.__table__),'\n',dir(self.table.__table__),'\n',dict(self.table.__table__))
        self.engine = create_engine(db.get_conn_url())
        try:
            self.engine.execute(CreateSchema(self.table.DbSchema))
            logging.debug("Creating Database Schema: {}".format(self.table.DbSchema))
        except:
            #logging.debug("Schema Already Exists No need to create:")
            pass
        #print("----tb2----",self.table.__table_args__,type(self.table.__table_args__))

        # create tables
        MetaBase.metadata.create_all(bind=self.engine)

        # create session
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session()

        # reflecting whole schema
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)

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
        except sqlalchemy.exc.IntegrityError as e:
            logging.warning("Duplicate Found: This library will ignore duplicate records")
        except:
            self.session.rollback()

    def __del__(self):
        try:
            self.session.close()
            logging.debug("Closing db_table.py Session")
        except:
            logging.error("Error Occured Closing db_table.py Session")


