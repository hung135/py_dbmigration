import logging
import pprint
from sqlalchemy import MetaData
import sqlalchemy

from sqlalchemy.schema import CreateSchema
import sqlalchemy
from sqlalchemy import exc as sqlachemy_exception
from sqlalchemy.ext.declarative import declarative_base
from .db_table_def import MetaBase, MetaSourceFiles


class RecordKeeper():

    engine_dict = {}
    table_dict = {}

    def __init__(self, db, table_def, appname=__file__):
        # type: (dbutils.conn, str, str) -> object
        """

        :rtype: 
        """
        #self.engine_dict = {}
        self.table_dict = {}
        self.host = db.host
        self.dbschema = 'logging'
        self.database = db.dbname
        self.appname = appname
        self.engine = None  # instance
        #assert isinstance(db, db_utils.DB)

        key = str(table_def.DbSchema + table_def.__tablename__)

        print("-------------------------------------------------",key)
        self.table = self.table_dict.get(key, None)
        print("-------------------------------------------------")
        if self.table is None:
            self.table_dict[key] = table_def
            self.table = self.table_dict[key]

        # db.connect_SqlAlchemy()
        sql_alchemy_uri_connected = db.sql_alchemy_uri.format(
            userid=db.userid,
            pwd=db.pwd,
            host=db.host,
            port=db.port,
            db=db.dbname,
            appname=appname
        )

        self.engine = sqlalchemy.create_engine(sql_alchemy_uri_connected)
        #self.engine = self.engine_dict['only1']

        # try:
        #     self.engine.execute(CreateSchema(self.table.DbSchema))
        #     logging.debug("Creating Database Schema: {}".format(self.table.DbSchema))
        # except sqlachemy_exception.ProgrammingError as e:
        #     logging.warning(e)

        MetaBase.metadata.create_all(bind=self.engine)

        # create session

        Session = sqlalchemy.orm.sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session(bind=self.engine)

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
                logging.exception(e)
                 
                self.session.rollback()

    def print_row(self, row):
        print(type(row), dir(row))
        for i in row.keys:
            print(i)

    def get_all_records(self):

        row = self.session.query(self.table).all()

        return row

    def get_record(self, *row):
        # update row to database
        row = self.session.query(MetaSourceFiles).filter(
            *row).order_by(MetaSourceFiles.id.desc()).first()

        return row

    def commit(self):
        try:
            self.session.commit()
        except sqlalchemy.exc.IntegrityError as e:
            logging.warning(
                "Duplicate Found: This library will ignore duplicate records")
            print(e)
        except:
            self.session.rollback()

    def close(self):
        logging.debug("Closing SqlAlchemy Engine: {}".format(self.appname))        
        try:

            print("xxxxxxxxxx-----")
            self.session.close()
            
            
            self.engine.dispose()

        except Exception as e:
            print("xxxxxxxxxx",e)
    def __del__(self):
        
        logging.debug("Closing db_table Session: {} {} {}".format(
            self.host, self.database, self.dbschema))
        try:
            self.session.close()

            self.engine.dispose()
        except Exception as e:
            print("----------------------",e)
            logging.exception(
                "Error Occured Closing db_table Session: {}".format(e))
            # print(e)
