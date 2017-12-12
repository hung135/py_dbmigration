from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import DateTime
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine.url import URL
import db_table

import os, socket, datetime
import logging
import pprint

from datetime import datetime

db_url = {'drivername': 'postgres',
          'username': 'postgres',
          'password': 'tester',
          'host': '192.168.99.101',
          'port': 5432}
engine = create_engine(URL(**db_url))
Base = declarative_base()


def test():
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.sql.expression import Insert

    @compiles(Insert, 'postgresql')
    def ignore_duplicates(insert, compiler, **kw):
        s = compiler.visit_insert(insert, **kw)
        ignore = insert.kwargs.get('postgresql_ignore_duplicates', False)
        return s if not ignore else s + ' ON CONFLICT DO NOTHING'

    Insert.argument_for('postgresql', 'ignore_duplicates', None)

    from sqlalchemy import insert
    stmt = insert(MyModel, postgresql_ignore_duplicates=True)


def make_hashable(d):
    return (frozenset(x.items()) for x in d)


class table:
    db_uri = None
    engine = None
    curr_pid = os.getpid()
    host = socket.gethostbyname(socket.gethostname())
    ttestdata = [
        {'file_name': 'Hi', 'file_path': '/home'},
        {'file_name': 'y4o', 'file_path': '/home'},
        {'file_name': 'y234o', 'file_path': '/home'},
        {'file_name': 'y11o', 'file_path': '/home'},
        {'file_name': 'Hixxx', 'file_path': '/home2'},
        {'file_name': 'Hixxx5', 'file_path': '/home3'},
        {'file_name': 'yo1', 'file_path': '/home'},
        {'file_name': 'yo3e2', 'file_path': '/home'},
        {'file_name': 'Hix32xx3', 'file_path': '/home2'},
        {'file_name': 'yo', 'file_path': '/home'},
        {'file_name': 'Hixxxx', 'file_path': '/home2'},
        {'file_name': 'Hixr3xx5', 'file_path': '/home3'},
        {'file_name': 'yo11', 'file_path': '/home'},
        {'file_name': 'yo2', 'file_path': '/home'},
        {'file_name': 'Hixxx3', 'file_path': '/home2'},

        {'file_name': 'Hixxx54', 'file_path': '/home3'}

    ]

    def __init__(self, db):
        # create table
        self.engine = create_engine(db.get_conn_url())
        self.meta = MetaData(bind=self.engine)
        self.schema = db.dbschema

        self.table = Table('meta_source_files2', self.meta, Column('id', Integer, primary_key=True),
                           Column('file_name', String),
                           Column('file_path', String), Column('embedded_id', String), Column('file_type', String),
                           Column('file_process_state', String), Column('process_start_dtm', DateTime),
                           Column('process_end_dtm', DateTime), Column('current_worker_host', String),
                           Column('current_worker_host_pid', Integer), Column('rows_inserted', Integer),
                           Column('data_appended', String), Column('file_size', Integer), Column('total_rows', Integer),
                           Column('total_files', Integer), Column('total_files_processed', Integer),
                           Column('last_error_msg', String), Column('database_table', String),
                           UniqueConstraint('file_name', 'file_name', name='idx_meta_source_files'),
                           schema=self.schema)

        self.meta.create_all()
        # session.merge(bike)
        # insert data via insert() construct
        self.conn = self.engine.connect()

        select_st = self.table.select()
        self.res = self.conn.execute(select_st)

        self.files = [{"file_name": _row.file_name, "file_path": _row.file_path} for _row in self.res]
        # print(_row.file_name, "------aaaah",type(_row))
        # a.append({"file_name": _row.file_name, "file_path": _row.file_path})

    def get_work(self):
        print(self.curr_pid)
        print(self.host)
        self.conn.execute(("""
                    update {0}.meta_source_files set
                    current_worker_host='{1}', current_worker_host_pid={2}, process_start_dtm=now()
                    where(file_path ||file_name) in (select file_path ||file_name
                        from {0}.meta_source_files where current_worker_host is null order by
                        file_size asc, embedded_id desc ,file_type asc limit 1)
                    """).format(self.schema, self.host, self.curr_pid))

    def add_files(self, new_files):

        diff = set(make_hashable(new_files)).difference(make_hashable(self.files))
        # newfiles=make_hashable(self.testdata)-make_hashable(a)

        aa = ([dict(x) for x in diff])

        for d in aa:
            # print(d)
            ins = self.table.insert().values(**d)

            try:
                self.conn.execute(ins)
                # conn.execute(table.insert(),self.testdata)

                logging.debug("New File Inserted")
                logging.debug(d)

            except Exception as e:
                logging.debug("Duplicate file: {}".format(d))
                logging.warning(e)

    def update_file(self):

        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.exc import SQLAlchemyError

        # create session
        Session = sessionmaker()
        Session.configure(bind=self.engine)
        session = Session()

        try:
            # check update correct
            # pprint.pprint(dir(self.table))
            row = self.table.select((self.table.c.current_worker_host_pid == self.curr_pid)
                                    & (self.table.c.current_worker_host == self.host)
                                    & (self.table.c.process_end_dtm == None)
                                    & (self.table.c.process_start_dtm != None)
                                    )
            # if row is not None:
            #    print(row, row.file_name,row.process_end_dtm, "------")

            r = session.query(self.table).filter(
                (self.table.c.current_worker_host_pid == self.curr_pid)).filter(
                (self.table.c.current_worker_host == self.host)). \
                filter((self.table.c.current_worker_host == self.host)). \
                filter((self.table.c.process_end_dtm == None)). \
                filter((self.table.c.process_start_dtm != None)).first()
            if r is not None:
                print(r)
                r.last_error_msg = ''
            else:
                print("Not Records found")

            # self.conn.execute("truncate table test.meta_source_files")

            # self.conn.execute("commit")
            # r.__setattr__("process_end_dtm",datetime.datetime.now())
            # for x in r:
            #   print(type(x),dir(x))
            # x.process_end_dtm = None

            # session.add(row)
            # session.commit()

            # for x in row:
            #   print(x.file_name)
            # row1 = session.query(self.table.).first()
            # print(row1)
            #    self.curr_pid == self.table.current_worker_host_pid).first()
            # print(row,"--xxxxx")
            #            row = session.query(self.table,self.meta).\
            #                filter(self.table.pid == self.curr_pid).\
            #                filter(self.table.host = self.host).first()

            #           print(row, "-----")


        except SQLAlchemyError as e:
            print(e)

        finally:
            session.close()

    def sample(self):

        from datetime import datetime

        from sqlalchemy import create_engine
        from sqlalchemy import Column, Integer, String, DateTime
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.exc import SQLAlchemyError
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.engine.url import URL

        db_url = {'drivername': 'postgres',
                  'username': 'postgres',
                  'password': 'tester',
                  'host': '192.168.99.101',
                  'port': 5432}
        engine = create_engine(URL(**db_url))
        Base = declarative_base()

        # create tables

        # create session
        Session = sessionmaker()
        Session.configure(bind=engine)
        session = Session()

        try:

            TestTable = db_table.table_def.meta_source_files(Base)
            Base.metadata.create_all(bind=engine)
            # update row to database
            row = session.query(TestTable).filter(
                TestTable.file_name == 'hello').first()
            print('original:', row.id, row.file_name)
            row.id = "Hello"
            row.name = "World"
            session.commit()

            # check update correct
            row = session.query(TestTable).filter(
                TestTable.key == 'Hello').first()

        except SQLAlchemyError as e:
            print(e)
        finally:
            session.close()
