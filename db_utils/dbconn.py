import os
import logging
import subprocess as commands
import datetime
import sys
import __future__
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from misc_utils import timer_decorator

# Decorator function to log and time how long a function took to run

class Connection:
    _conn = None
    _cur = None
    _password = None
    _userid = None
    _sslmode = None
    _host = None
    _port = None
    _database_name = None
    _commit = True
    _dbtype = None
    _db_url = None
    last_row_count = 0
    dbschema = None

    def connect_sqlalchemy(self, schema=None, db=None):
        import sqlalchemy
        #import pymssql
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis
        url = ""
        if db is None and self._dbtype is None:
            db = "POSTGRES"
        else:
            db = self._dbtype
        if schema is None:
            schema = self.dbschema

        if db.upper() == "POSTGRES":

            url = 'postgresql://{}:{}@{}:{}/{}'
            url = url.format(self._userid, self._password, self._host, self._port, self._database_name)
        if db.upper() == "MSSQL":
          
            url = 'mssql+pymssql://{}:{}@{}:{}/{}'
            url = url.format(self._userid, self._password, self._host,self._port, self._database_name)
        #con=self._connect_mssql()
        # The return value of create_engine() is our connection object
        con = sqlalchemy.create_engine(url)

        # We then bind the connection to MetaData()
        meta = sqlalchemy.MetaData(bind=con, reflect=True, schema=self.dbschema)

        return con, meta

    def print_drop_tables(self):

        con, meta = self.connect_sqlalchemy(self.dbschema, self._dbtype)
        for n, t in meta.tables.items():
            #print(type(n), n, t.name)
            print ("drop table if exists {}.{} cascade;".format(self.dbschema,t.name))
              

    def pandas_dump_table_csv(self, table_list, folder, chunksize=100000):
        # def get_pandas_frame(self, table_name,rows=None):
        import pandas as pd
        import migrate_utils as migu
        conn, meta = self.connect_sqlalchemy()

        # z=db.get_pandas_frame('errorlog')
        for t in table_list:
            z = pd.read_sql_table(t, conn, schema=self.dbschema, index_col=None,
                                  coerce_float=True, parse_dates=None, columns=None, chunksize=chunksize)
            # with open(folder+"/"+t+".csv","wb") as f:
            filename = "{}_{}_{}.csv".format(self._database_name, self.dbschema, t)
            full_file_path = folder + "/" + filename
            # print(type(z),filename)
            i = 0
            with open(full_file_path, "wb") as f:
                print(bytes("Dumping to CSV: {}".format(t), "utf-8"))
                for df in z:
                    i += 1

                    if i == 1:
                        # print(list(df.columns))
                        column_list_snake = migu.convert_list_to_snake_case(list(df.columns))
                        # print(column_list_snake)
                        df.columns = column_list_snake
                        df.to_csv(full_file_path, index=False, header=True)
                        print("Records Dumped: {}".format(len(df.index)))
                    else:
                        df.to_csv(full_file_path, mode="a", index=False)
                        print("Records Dumped: {}".format(i * chunksize))
                    # print(dir(df),type(df))

    def dump_tables_csv(self, table_list, folder):
        import csv
        import sqlalchemy

        con, meta = self.connect_sqlalchemy(self.dbschema, self._dbtype)
        # print dir(meta.tables)
        for t in table_list:
            # print(type(n), n, t.name)
            table = sqlalchemy.Table(t, meta, autoload=True, autoload_with=con)
            select = sqlalchemy.sql.select([table]).limit(10)
            result = con.execute(select)
            print(result.keys())
            with open(folder + "/" + t + ".csv", "wb") as f:
                out = csv.writer(f)
                out.writerow(result.keys())
                out.writerow(result)

    def print_tables(self, table_list):
        import sqlalchemy

        print(self.dbschema, self._dbtype)
        con, meta = self.connect_sqlalchemy(self.dbschema, self._dbtype)
        # print dir(meta.tables)
        for t in table_list:
            # print(type(n), n, t.name)
            try:
                table = sqlalchemy.Table(t, meta, autoload=True, autoload_with=con)
                stmt = sqlalchemy.schema.CreateTable(table)
                print(str(stmt) + ";")
            except:
                #print("Cannot Find Table: {}".format(t))
                logging.Error("Cannot Find Table: {}".format(t))

    def print_create_table(self, folder=None):
        import migrate_utils as mig
        import sqlalchemy

        con, meta = self.connect_sqlalchemy(self.dbschema, self._dbtype)
        # print dir(meta.tables)
        table_count = 0
        for n, t in meta.tables.items():
            table_count += 1
            # print(type(n), n, t.name)
            table = sqlalchemy.Table(
                t.name, meta, autoload=True, autoload_with=con)
            stmt = sqlalchemy.schema.CreateTable(table)
            column_list = [c.name for c in table.columns]
            createsql = mig.convert_snake_case(str(stmt), column_list)
            if folder is None:
                print(str(createsql) + ";")
            else:
                with open(folder + "/" + t.name + ".sql", "wb") as f:
                    f.write(createsql + ";")
        print("Total Tables:{}".format(table_count))

    def get_table_columns(self, table_name):
        import sqlalchemy
        con, meta = self.connect_sqlalchemy()
        table = sqlalchemy.Table(
            table_name, meta, schema='stg', autoload=True, autoload_with=con)
        return [c.name for c in table.columns]

    def query(self, sqlstring):
        """ runs query or procedure that returns record set
        """
        if sqlstring.lower().startswith('select') or sqlstring.lower().startswith('call'):
            self._cur.execute(sqlstring)
            rows = self._cur.fetchall()
        else:
            raise Exception('Only Selects allowed')
        return rows

    def insert_table(self, table_name, column_list, values, onconflict=''):
        sqlstring = "Insert into " + table_name + \
            " (" + column_list + ") values " + values + " " + onconflict
        self._cur.execute(sqlstring)
        self.commit()
        self.last_row_count = self._cur.rowcount

    def commit(self):
        """ Default to commit after every transaction
                Will check instance variable to decide if a commit is needed
        """
        if self._commit:
            self._cur.execute("commit")

    def vacuum(self, table_name=None):
        """ Default to commit after every transaction
                Will check instance variable to decide if a commit is needed
        """
        if table_name is None:
            self._cur.execute("vacuum")
            logging.debug("Vacuuming Schema")

        else:
            logging.debug("Vacuuming Table:{0}".format(table_name))
            self._cur.execute("vacuum {0}".format(table_name))

    def execute(self, sqlstring, debug=False):

        logging.debug("Debug DB Execute: {0}".format(sqlstring))
        if sqlstring.lower().startswith('call '):
            self._cur.execute(sqlstring)
            self.commit()
        else:
            raise Exception('Only Calls to stored functions allowed')

    def drop_schema(self, schema):
        logging.debug("Drop Database Schema: \n\tHost:{0}\n\tDatabase:{1}\nSchema:{2}".format(self._host,
                                                                                              self._database_name, schema))
        print(self._cur.execute('drop schema {0} cascade'.format(schema)))

        self.commit()

    def truncate_table(self, table_name):
        logging.debug(
            "Truncating Table: \n\tHost:{0}\n\tDatabase:{1}\n\tTablename:{2}".format(
                self._host, self._database_name, table_name))
        self._cur.execute('TRUNCATE table {0}'.format(table_name))

        self.commit()
        self.vacuum(table_name)

    def update(self, sqlstring):
        self._cur.execute(sqlstring)
        self.commit()

    def drop_table(self, table_name):
        logging.debug("Dropping Table(if Exists): {0}".format(table_name))
        self._cur.execute('Drop table if exists {0}'.format(table_name))
        self.commit()

    def create_table(self, sqlstring):
        if sqlstring.lower().startswith('create table '):
            self._cur.execute(sqlstring)
            self.commit()
        else:
            raise Exception('create tables functions allowed')

    def _connect_postgres(self):
        import psycopg2

        try:
            self._password = os.environ['PGPASSWORD']
            self._userid = os.environ['PGUSER']
            self._sslmode = os.environ['PGSSLMODE']
            if self._host is None:
                self._host = os.environ['PGHOST']
            if self._port is None:
                self._port = os.environ['PGPORT']
            if self._database_name is None:
                self._database_name = os.environ['PGDATABASE']

        except Exception:
            logging.info(Exception)
            sys.exit()

        if self._port == '':
            self._port = 5432
        conn = psycopg2.connect(dbname=self._database_name, user=self._userid,
                                password=self._password, port=self._port, host=self._host)
        conn.set_client_encoding('UNICODE')
        return conn

    def _connect_mssql(self):
        import pymssql

        try:
            self._password = os.environ['MSPASSWORD']
            self._userid = os.environ['MSUSER']
            if self._host is None:
                self._host = os.environ['MSSQLHOST']
            if self._port is None:
                self._port = os.environ['MSPORT']
            if self._database_name is None:
                self._database_name = os.environ['MSDATABASE']
        except Exception:
            logging.error("Error Getting Environment Variables MSSQL:\nUser:{}\nHost:{}\nPort:{}\nDB:{}".format(
                self._userid, self._host, self._port, self._database_name))
            sys.exit()

        if self._port == '':
            self._port = 1433

        conn = pymssql.connect(server=self._host, user=self._userid, password=self._password,
                               database=self._database_name,
                               host=self._host,
                               port=self._port,
                               conn_properties=None,
                               timeout=0, login_timeout=60, charset='UTF-8', as_dict=False,
                               appname=None, autocommit=self._commit, tds_version='7.1'
                               )

        return conn

    def _connect_mysql(self):
        import pymysql
        conn = pymysql.connect(host=self._host, port=self._port,
                               user=self._userid, passwd=self._password, db=self._database_name)
        return conn

    def _connect_oracle(self):
        pass

    def _connect_ldap(self):
        pass

    def __init__(self, dbschema, commit=True, password=None, userid=None,
                 host=None, port=None, database=None, dbtype='POSTGRES'):
        """ Default to commit after every transaction
        """
        self._commit = commit
        self.dbschema = dbschema
        if database is not None:
            self._database_name = database
        if port is not None:
            self._port = port
        if userid is not None:
            self._userid = userid
        if password is not None:
            self._password = password
        if host is not None:
            self._host = host

        logging.debug("DB Connecting To: {0}:{1}:{2}".format(self._host, self._database_name, dbtype))
        self._dbtype = dbtype.upper()
        if self._dbtype == 'POSTGRES':
            self._conn = self._connect_postgres()
        if self._dbtype == 'MSSQL':

            self._conn = self._connect_mssql()
        if self._dbtype == 'MYSQL':
            self._conn = self._connect_mysql()
        if self._dbtype == 'ORACLE':
            self._conn = self._connect_oracle()
        self._cur = self._conn.cursor()
        logging.debug("DB Connected To: {0}:{1}:{2}".format(self._host, self._database_name, dbtype))

    def __del__(self):
        self.commit()
        self._cur.close()
        self._conn.close()
        logging.debug("Closed DB Connection: {0}:{1}:{2}".format(self._host, self._database_name, self._dbtype))

    def copy_to_csv(self, sqlstring, full_file_path, delimiter):
        # save the Current shell password
        prev_password = os.environ['PGPASSWORD']
        os.environ['PGPASSWORD'] = self._password
        # copy_command_client_side = """psql -c "\copy {0} FROM '{1}' with (format csv, delimiter '{2}')" """
        shell_command = """psql -c "\copy ({0}) to '{1}' WITH DELIMITER AS '{2}' CSV QUOTE AS '\\"' " """

        command_text = (shell_command.format(sqlstring, full_file_path, delimiter))

        prev_password = os.environ['PGPASSWORD']

        logging.info("Dumping Data to CSV STARTED:{0}".format(full_file_path))
        logging.debug("SQL:{0}".format(sqlstring))
        logging.debug("Command string: {}".format(command_text))
        txt_out = commands.getstatusoutput(command_text)
        logging.debug("Dumping Data to CSV COMPLETED:{0}".format(txt_out))
        # put the password back
        os.environ['PGPASSWORD'] = prev_password
        if txt_out[0] > 0:
            raise Exception
        i = txt_out[1].split()
        logging.info("Total Rows Dumped: {0}".format(i[1]))
        return i[1]

    def import_file_client_side(self, full_file_path, table_name, file_delimiter):
        copy_command_client_side = """psql --dbname={3} --host={4} -c "\copy {0} FROM '{1}' with (format csv, delimiter '{2}')" """
        t = db_logging.DbLogging(self)
        data_file = full_file_path
        error_log_entry = t.ErrorLog(
            program_unit=sys.argv[0],
            error_code=None,
            error_message=None,
            error_timestamp=None,
            user_name=self._userid,
            sql_statement='')
        log_entry = t.LoadStatus(table_name=table_name,
                                 program_unit=sys.argv[0],
                                 program_unit_type_code='python',
                                 file_path=data_file,
                                 records_inserted=0,
                                 success=1,
                                 start_date=dt.datetime.now(),
                                 end_date=dt.datetime.now(),
                                 previous_record_count=0,
                                 current_record_count=0,
                                 records_updated=0,
                                 records_deleted=0,
                                 created_by=self._userid,
                                 created_date=dt.datetime.now())

        t = dt.datetime.now()

        command_text = copy_command_client_side.format(
            table_name, data_file, file_delimiter, self._database_name, self._host)
        logging.info("Copy Command STARTED:{0} Time:{1}".format(table_name, t))
        txt_out = commands.getstatusoutput(command_text)
        logging.debug("Copy Command Completed:{0} Time:{1}".format(
            txt_out, dt.datetime.now()))
        logging.info("Total Time:{0} ".format(dt.datetime.now() - t))

        if txt_out[0] > 0:
            raise Exception

        i = txt_out[1].split()
        logging.info("Total Rows Loaded: {0}".format(i[1]))
        return i[1]

    def get_conn_url(self):
        url = 'postgresql://{}:{}@{}:{}/{}'
        url = url.format(self._userid, self._password,
                         self._host, self._port, self._database_name)
        return url

    # this one breaks w/ sqlserver
    def get_table_list(self, dbschema=None):
        Base = automap_base()

        #from sqlalchemy.orm import Session
        schema = dbschema
        if dbschema is None:
            schema = self.dbschema

        con, meta = self.connect_sqlalchemy(schema=schema)
        Base.prepare(con, reflect=True, schema=schema)
        l = []
        for t, k in Base.classes.items():
            # print(t,(k.__dict__))
            # for m in k:
            l.append(t)
        return l

    def get_columns(self, table_name, dbschema):
        import migrate_utils as mig
        import sqlalchemy
        from sqlalchemy.dialects import postgresql

        con, meta = self.connect_sqlalchemy(dbschema, self._dbtype)
        # print dir(meta.tables)

        #print(n, t.name)

        # print(type(n), n, t.name)
        print(table_name, "-----------------", dbschema)
        table = sqlalchemy.Table(
            table_name, meta, autoload=True, autoload_with=con)
        print(table)
        column_list = [c.name for c in table.columns]
        print(column_list, "-----------------")
        return list(column_list)

    # returns a list of table dict
    def get_tables(self):
        import migrate_utils as mig
        import sqlalchemy
        from sqlalchemy.dialects import postgresql

        con, meta = self.connect_sqlalchemy(self.dbschema, self._dbtype)
        # print dir(meta.tables)

        table_obj = []
        for n, t in meta.tables.items():

            #print(n, t.name)

             # print(type(n), n, t.name)
            table = sqlalchemy.Table(
                t.name, meta, autoload=True, autoload_with=con)
            column_list = [c.name for c in table.columns]
            d = dict({"db": self._database_name, "schema": self.dbschema, "table_name": t.name, "columns": column_list})
            table_obj.append(d)

        return table_obj

    def get_table_row_count_fast(self, table_name, schema=None):
        x = 0
        if dbtype == 'POSTGRES':
            db.vacuum(table_name)
            row = self.query("""select n_live_tup 
                    from pg_stat_user_tables 
                    where schemaname='{}' and relname='{}'""".format(schema, table_name))
            x = row[0][0]

        return x

    def get_tables_row_count(self, schema=None):
        import migrate_utils as mig
        from sqlalchemy.dialects import postgresql
        if schema is None:
            schema = self.dbschema
        con, meta = self.connect_sqlalchemy(schema, self._dbtype)
        # print dir(meta.tables)

        table_obj = []
        for n, t in meta.tables.items():

            #print(n, t.name)

            #print(type(n), n, dir(t))
            x = self.query("select count(*) from {}".format(t.key))
            rowcount = x[0][0]
            # print(type(rowcount),dir(rowcount))
            d = dict({"db": self._database_name, "schema": self.dbschema, "table_name": t.name, "row_counts": rowcount})
            table_obj.append(d)

        return table_obj

    def print_table_info(self, table_name, dbschema):
        Base = automap_base()
        # from sqlalchemy.orm import Session

        Base.prepare(self.engine, reflect=True, schema=dbschema)
        l = eval('Base.classes.{}'.format(table_name))
        for m in l.__table__.columns:
            print(m, m.type)
    # this is only in this class for convience atm, should be moved out eventually

    def get_pandas_frame(self, table_name, rows=None):
        import pandas as pd
        conn, meta = self.connect_sqlalchemy()
        z = pd.read_sql_table(table_name, conn, schema=self.dbschema, index_col=None,
                              coerce_float=True, parse_dates=None, columns=None, chunksize=rows)
        return z

    def put_pandas_frame(self, table_name, df):
        import pandas as pd
        conn, meta = self.connect_sqlalchemy()
        z = df.to_sql(table_name, conn, schema=self.dbschema, index=False,
                      if_exists='append', chunksize=None, dtype=None)
        return z