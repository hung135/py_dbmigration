import logging as lg
import pprint
from sqlalchemy import MetaData
import sqlalchemy

from sqlalchemy.schema import CreateSchema
import sqlalchemy
from sqlalchemy import exc as sqlachemy_exception
from sqlalchemy.ext.declarative import declarative_base
from .db_table_def import MetaBase, MetaSourceFiles
import inspect

logging=lg.getLogger('RecordKeeper')

class RecordKeeper():

    engine_dict = {}
    table_dict = {}
    table_def = None
    session = None
    def __init__(self, db, table_def, appname=__file__):
        # type: (dbutils.conn, str, str) -> object
        """

        :rtype: 
        """
 
        self.table_def=table_def
        #self.engine_dict = {}
        self.table_dict = {}
        self.host = db.host
        self.dbschema = 'logging'
        self.database = db.dbname
        self.appname = appname
        self.engine = None  # instance
        #assert isinstance(db, db_utils.DB)
        
        key = str(table_def.DbSchema + table_def.__tablename__)
     
        self.table = self.table_dict.get(key, None)
        if self.table is None:
            self.table_dict[key] = table_def
            self.table = self.table_dict[key]
        else:
            raise("Error only one instace allowed: Fix your code")

        # db.connect_SqlAlchemy()
        sql_alchemy_uri_connected = db.sql_alchemy_uri.format(
            userid=db.userid,
            pwd=db.pwd,
            host=db.host,
            port=db.port,
            db=db.dbname,
            appname='sqlAlch_'+ appname
        )
        logging.debug("Opening SqlAlchemy Engine: {}".format(self.appname))   
        self.engine = sqlalchemy.create_engine(sql_alchemy_uri_connected)
        
        #self.engine = self.engine_dict['only1']

        # try:
        #     self.engine.execute(CreateSchema(self.table.DbSchema))
        #     logging.debug("Creating Database Schema: {}".format(self.table.DbSchema))
        # except sqlachemy_exception.ProgrammingError as e:
        #     logging.warning(e)

        #MetaBase.metadata.create_all(bind=self.engine)
        #print(x)
        # create session

        Session = sqlalchemy.orm.sessionmaker()
        Session.configure(bind=self.engine)
        self.session = Session(bind=self.engine) 
        # reflecting whole schema
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
        
        self.table_def.metadata.create_all(bind=self.engine)
    def delete_record(self, row, commit=False):

        # remove row from database

        self.session.delete(row)

        if commit:
            self.commit()
                  
    def add_record(self, row, commit=False):

        # add row to database

        self.session.add(row)
        if commit:
            self.commit()

    def print_row(self, row):
        print(type(row), dir(row))
        for i in row.keys:
            print(i)

    def get_all_records(self):
        row = self.session.query(self.table).all()
        return row

    def get_record(self, *row,obj=MetaSourceFiles):
        # update row to database
        r = self.session.query(obj).filter(*row).first()
         
        return r

    def commit(self):
        try:
            self.session.commit()
        except Exception as e:
            logging.exception(e)
            # self.session.rollback()
            # self.session.begin()

    def close(self):
        logging.debug(inspect.stack()[1].function)
        logging.debug("Closing SqlAlchemy Engine: {}".format(self.appname))        
        try: 
            self.commit() 
        except Exception :
            logging.error('Error committing')
        try: 
            self.session.close() 
        except Exception :
            logging.error('Error Closing Session')
    def __del__(self):
        
        logging.debug("Out of Scope Deleting RecordKeeper")
        self.close()
            # print(e)
