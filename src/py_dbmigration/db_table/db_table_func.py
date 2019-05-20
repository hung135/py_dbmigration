import logging
import pprint
from sqlalchemy import MetaData
import sqlalchemy

from sqlalchemy.schema import CreateSchema
import sqlalchemy
import db_utils
import migrate_utils

from .db_table_def import *


class RecordKeeper():
    engine_dict = {}  # using dict because class level variable not getting set
    table_dict = {}

    def __init__(self, db, table_def):
        # type: (dbutils.conn, str, str) -> object
        """

        :rtype: 
        """
        self.host = db._host
        self.dbschema = db.dbschema
        self.database = db._database_name
        db_url = None
        self.engine = None  # instance
        #assert isinstance(db, db_utils.dbconn.Connection)

        key = str(table_def.DbSchema + table_def.__tablename__)

        self.table = self.table_dict.get(key, None)
        if self.table is None:
            self.table_dict[key] = table_def
            self.table = self.table_dict[key]

        schema = self.table.DbSchema

        # call class method to make sure url attribute is set
        if self.engine_dict.get('only1', None) is None:

            db.connect_sqlalchemy(schema, db._dbtype)
            self.engine_dict['only1'] = sqlalchemy.create_engine(db.url)
            self.engine = self.engine_dict['only1']

            try:
                self.engine.execute(CreateSchema(self.table.DbSchema))
                logging.debug("Creating Database Schema: {}".format(self.table.DbSchema))
            except:
                # logging.debug("Schema Already Exists No need to create:")
                pass

            # create tables
            ""
            MetaBase.metadata.create_all(bind=self.engine)
        else:

            self.engine = self.engine_dict['only1']
        # create session

        Session = sqlalchemy.orm.sessionmaker()
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
                self.session.commit()

            except Exception as e:
                logging.error(e)
                print(e)
                self.session.rollback()

    def print_row(self, row):
        print(type(row), dir(row))
        for i in row.keys:
            print(i)

    def get_all_records(self):
        # print(self.table.__tablename__,"zzzzzzzzz")
        row = self.session.query(self.table).all()

        return row

    def get_record(self, *row):
        # update row to database
        row = self.session.query(MetaSourceFiles).filter(*row).order_by(MetaSourceFiles.id.desc()).first()

        return row

    def commit(self):
        try:
            self.session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            logging.warning("Duplicate Found: This library will ignore duplicate records")
            print(e)
        except:
            self.session.rollback()

    def __del__(self):
        try:
            self.session.close()
            logging.debug("Closing db_table Session: {} {} {}".format(self.host, self.database, self.dbschema))
        except Exception as e:
            logging.error("Error Occured Closing db_table Session: {}", e)
            # print(e)