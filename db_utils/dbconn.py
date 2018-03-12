import logging
import os
# import subprocess as commands
import commands
import sys
import datetime
from sqlalchemy.ext.automap import automap_base
import migrate_utils


# Decorator function to log and time how long a function took to run

class Connection:
    # _conn = None this need to be instance
    # _cur = None
    # _password = None
    # _userid = None
    # _sslmode = None
    # _host = None
    # _port = 5432
    # _database_name = None
    # _commit = True
    # _dbtype = None
    # _db_url = None
    # last_row_count = 0
    # dbschema = None
    #_sqlalchemy_con = None this needs to be instance
    #_sqlalchemy_meta = None this needs to be instance

    def __init__(self, dbschema, commit=True, password=None, userid=None, host=None, port=None, database=None,
                 dbtype='POSTGRES', appname='py_dbutils'):
        """ Default to commit after every transaction
        """

        self._commit = commit
        self.dbschema = dbschema
        self.appname = appname
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
        self._sslmode = None

        logging.debug("DB Connecting To: {0}:{1}:{2}".format(self._host, self._database_name, dbtype))
        self._dbtype = dbtype.upper()
        if self._dbtype == 'POSTGRES':
            self._conn = self._connect_postgres
        if self._dbtype == 'MSSQL':
            self._conn = self._connect_mssql()
        if self._dbtype == 'MYSQL':
            self._conn = self._connect_mysql()
        if self._dbtype == 'ORACLE':
            self._conn = self._connect_oracle()
        self._cur = self._conn.cursor()
        self.url = None  # db url
        logging.debug("DB Connected To: {0}:{1}:{2}".format(self._host, self._database_name, dbtype))

        self._sqlalchemy_con = None
        self._sqlalchemy_meta = {}


    #@migrate_utils.static_func.timer
    def connect_sqlalchemy(self, schema=None, db=None):
        import sqlalchemy
        # import pymssql
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis

        if db is None and self._dbtype is None:
            db = "POSTGRES"
        else:
            db = self._dbtype
        if schema is None:
            schema = self.dbschema

        if self._sqlalchemy_con is None or self._sqlalchemy_meta.get(schema,None) is None:


            if db.upper() == "POSTGRES":
                self.url = 'postgresql://{}:{}@{}:{}/{}'
                self.url = self.url.format(self._userid, self._password, self._host, self._port, self._database_name)

                if self._sqlalchemy_con is None:
                    self._sqlalchemy_con = sqlalchemy.create_engine(self.url,
                                                                    connect_args={"application_name": self.appname})
            if db.upper() == "MSSQL":
                self.url = 'mssql+pymssql://{}:{}@{}:{}/{}'
                self.url = self.url.format(self._userid, self._password, self._host, self._port, self._database_name)
            if db.upper() == "MYSQL":
                # 'mysql+pymysql://root:test@192.168.99.100:3306/mysql'
                self.url = "mysql+pymysql://{}:{}@{}:{}/{}"
                self.url = self.url.format(self._userid, self._password, self._host, self._port, self._database_name)
                self._sqlalchemy_con = sqlalchemy.create_engine(self.url)

            # con=self._connect_mssql()
            # The return value of create_engine() is our connection object

            # We then bind the connection to MetaData()
            # print('connecting schema:', schema)
            if self._sqlalchemy_meta.get(schema,None) is None:
                self._sqlalchemy_meta[schema] = sqlalchemy.MetaData(bind=self._sqlalchemy_con, reflect=True, schema=schema)

        return self._sqlalchemy_con, self._sqlalchemy_meta[schema]

    def print_drop_tables(self):

        con, meta = self.connect_sqlalchemy(self.dbschema, self._dbtype)
        for n, t in meta.tables.items():
            # print(type(n), n, t.name)
            print("drop table if exists {}.{} cascade;".format(self.dbschema, t.name))

    def pandas_dump_table_csv(self, table_list, folder, chunksize=100000):
        # def get_pandas_frame(self, table_name,rows=None):
        import pandas as pd
        import migrate_utils as migu
        conn, meta = self.connect_sqlalchemy()

        # z=db.get_pandas_frame('errorlog')
        for t in table_list:
            z = pd.read_sql_table(t, conn, schema=self.dbschema, index_col=None, coerce_float=True, parse_dates=None,
                                  columns=None, chunksize=chunksize)
            # with open(folder+"/"+t+".csv","wb") as f:
            filename = "{}_{}_{}.csv".format(self._database_name, self.dbschema, t)
            full_file_path = folder + "/" + filename
            # print(type(z),filename)
            i = 0
            with open(full_file_path, "wb") as f:
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
                        print("Records Dumped: {}".format(i * chunksize))  # print(dir(df),type(df))

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
                # print("Cannot Find Table: {}".format(t))
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
            table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
            stmt = sqlalchemy.schema.CreateTable(table)
            column_list = [c.name for c in table.columns]
            createsql = mig.convert_snake_case(str(stmt), column_list)
            if folder is None:
                print(str(createsql) + ";")
            else:
                with open(folder + "/" + t.name + ".sql", "wb") as f:
                    f.write(createsql + ";")
        print("Total Tables:{}".format(table_count))

    def get_table_column_types(self, table_name, trg_schema=None):

        import sqlalchemy
        if trg_schema is None:
            schema = self.dbschema
        else:
            schema = trg_schema
        con, meta = self.connect_sqlalchemy()
        table = sqlalchemy.Table(table_name, meta, schema=schema, autoload=True, autoload_with=con)

        return table.columns

    def get_table_columns(self, table_name, trg_schema=None):

        import sqlalchemy
        if trg_schema is None:
            schema = self.dbschema
        else:
            schema = trg_schema
        con, meta = self.connect_sqlalchemy()
        table = sqlalchemy.Table(table_name, meta, schema=schema, autoload=True, autoload_with=con)

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
        sqlstring = "Insert into " + table_name + " (" + column_list + ") values " + values + " " + onconflict
        self._cur.execute(sqlstring)
        self.commit()
        self.last_row_count = self._cur.rowcount

    def commit(self):
        """ Default to commit after every transaction
                Will check instance variable to decide if a commit is needed
        """
        if self._commit:
            self._cur.execute("commit")

    def vacuum(self, dbschema=None, table_name=None):
        """ Default to commit after every transaction
                Will check instance variable to decide if a commit is needed
        """
        if table_name is None or table_name == '':
            self._cur.execute("vacuum")
            logging.debug("Vacuuming Schema")

        else:
            logging.debug("Vacuuming Table:{0}".format(table_name))
            self._cur.execute("vacuum {0}.{1}".format(dbschema, table_name))

    def execute(self, sqlstring, debug=False):

        logging.debug("Debug DB Execute: {0}".format(sqlstring))
        try:
            self._cur.execute(sqlstring)
            self._cur.execute("commit")
            self.commit()
        except Exception as e:
            print("Error Execute SQL:{}".format(e))

    def drop_schema(self, schema):
        logging.debug(
            "Drop Database Schema: \n\tHost:{0}\n\tDatabase:{1}\nSchema:{2}".format(self._host, self._database_name,
                                                                                    schema))
        print(self._cur.execute('drop schema {0} cascade'.format(schema)))

        self.commit()

    def truncate_table(self, dbschema, table_name):
        logging.debug(
            "Truncating Table: \n\tHost:{0}\n\tDatabase:{1}\n\tTablename:{2}".format(self._host, self._database_name,
                                                                                     table_name))
        self._cur.execute('TRUNCATE table {0}.{1}'.format(dbschema, table_name))

        self.commit()
        self.vacuum(dbschema, table_name)

    def update(self, sqlstring):
        self._cur.execute(sqlstring)
        self.commit()

    def drop_table(self, schema, table_name):
        logging.debug("Dropping Table(if Exists): {0}.{1}".format(schema, table_name))
        self._cur.execute('Drop table if exists {0}.{1}'.format(schema, table_name))
        self.commit()

    def create_table(self, sqlstring):
        if sqlstring.lower().startswith('create table '):
            self._cur.execute(sqlstring)
            self.commit()
        else:
            raise Exception('create tables functions allowed')

    @property
    def _connect_postgres(self):
        import psycopg2

        try:
            if self._password is None:
                self._password = os.getenv('PGPASSWORD', 'tester')
            if self._userid is None:
                self._userid = os.getenv('PGUSER', 'postgres')
            if self._sslmode is None:
                self._sslmode = os.getenv('PGSSLMODE', None)
            if self._host is None:
                self._host = os.getenv('PGHOST', '192.168.99.100')
            if self._port is None:
                self._port = os.getenv('PGPORT', 5432)
            if self._database_name is None:
                self._database_name = os.getenv('PGDATABASE', 'postgres')

        except Exception as e:
            logging.error(e)
            sys.exit()

        if self._port == '':
            self._port = 5432
        conn = psycopg2.connect(dbname=self._database_name, user=self._userid, password=self._password, port=self._port,
                                host=self._host, application_name=self.appname)
        conn.set_client_encoding('UNICODE')
        return conn

    def _connect_mssql(self, appname='py_dbutils'):
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
            logging.error(
                "Error Getting Environment Variables MSSQL:\nUser:{}\nHost:{}\nPort:{}\nDB:{}".format(self._userid,
                                                                                                      self._host,
                                                                                                      self._port,
                                                                                                      self._database_name))
            sys.exit()

        if self._port == '':
            self._port = 1433

        conn = pymssql.connect(server=self._host, user=self._userid, password=self._password,
                               database=self._database_name, host=self._host, port=self._port, conn_properties=None,
                               timeout=0, login_timeout=60, charset='UTF-8', as_dict=False, appname=appname,
                               autocommit=self._commit, tds_version='7.1')

        return conn

    def _connect_mysql(self):
        print("Connecting to mysql:")
        # import mysql.connector
        import pymysql

        conn = pymysql.connect(user=self._userid, password=self._password,
                               host=self._host,
                               database=self._database_name)

        print("coneected")
        return conn

    def _connect_oracle(self):
        pass

    def _connect_ldap(self):
        pass


    def __del__(self):
        try:
            self.commit()
            self._cur.close()
            self._conn.close()
            logging.debug("Closed DB Connection: {0}:{1}:{2}".format(self._host, self._database_name, self._dbtype))
        except:
            pass

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



    def get_conn_url(self):

        return self.url

    # @migrate_utils.static_func.timer
    def get_table_list_via_query(self, dbschema):
        sql = """SELECT table_name FROM information_schema.tables a
            WHERE table_schema='{}' and table_type='BASE TABLE'""".format(dbschema)
        result_set = self.query(sql)
        return [r[0] for r in result_set]

    # @migrate_utils.static_func.timer
    def get_all_columns_schema(self, dbschema, table_name):
        # print("----- wuh")
        sql = """SELECT table_name,column_name,upper(data_type) as type, 
        is_identity,
        character_maximum_length 
        FROM information_schema.columns
        WHERE table_schema = '{}'
        AND table_name   = '{}'
        order by table_name,ordinal_position""".format(dbschema, table_name)
        result_set = self.query(sql)
        table = []
        for table_name, column, type, autoincrement, length in result_set:
            class data: pass

            data.table_name = table_name
            data.column_name = column
            data.type = type
            data.autoincrement = autoincrement
            data.length = length

            table.append(data)

        return table

    # this one breaks w/ sqlserver
    @migrate_utils.static_func.timer
    def get_table_list(self, dbschema=None):
        print("getting schema: {}".format(dbschema))
        Base = automap_base()

        # from sqlalchemy.orm import Session
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

    #@migrate_utils.static_func.timer
    def get_columns(self, table_name, dbschema):
        """

        :rtype: object
        """
        # type: (str, str) -> list
        import sqlalchemy

        con, meta = self.connect_sqlalchemy(dbschema, self._dbtype)
        table = sqlalchemy.Table(table_name, meta, autoload=True, autoload_with=con)
        column_list = [c.name for c in table.columns]
        return list(column_list)

    # returns a list of table dict
    #@migrate_utils.static_func.timer
    def get_tables(self, schema=None):
        import sqlalchemy
        dbschema = self.dbschema
        if schema is not None:
            dbschema = schema

        con, meta = self.connect_sqlalchemy(dbschema, self._dbtype)

        table_obj = []
        for n, t in meta.tables.items():
            table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
            column_list = [c.name for c in table.columns]
            d = dict({"db": self._database_name, "schema": dbschema, "table_name": t.name, "columns": column_list})
            table_obj.append(d)

        return table_obj

    @migrate_utils.static_func.timer
    def get_table_row_count_fast(self, table_name, schema=None):
        x = 0
        if self.dbtype == 'POSTGRES':
            self.vacuum(table_name)
            row = self.query("""select n_live_tup 
                    from pg_stat_user_tables 
                    where schemaname='{}' and relname='{}'""".format(schema, table_name))
            x = row[0][0]

        return x

    def get_tables_row_count(self, schema=None):
        if schema is None:
            schema = self.dbschema
        con, meta = self.connect_sqlalchemy(schema, self._dbtype)
        # print dir(meta.tables)

        table_obj = []
        for n, t in meta.tables.items():
            # print(n, t.name)

            # print(type(n), n, dir(t))
            x = self.query("select count(*) from {}".format(t.key))
            rowcount = x[0][0]
            # print(type(rowcount),dir(rowcount))
            d = dict({"db": self._database_name, "schema": self.dbschema, "table_name": t.name, "row_counts": rowcount})
            table_obj.append(d)

        return table_obj

    # given a table name we return the a list of columns that are part of the primary key
    def get_primary_keys(self, table_name):
        sql = """SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                     AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = '{}'::regclass
            AND    i.indisprimary;""".format(table_name)
        result = self.query(sql)
        field_list = []
        for r in result:
            field_name, data_type = r
            field_list.append(field_name)
        return field_list

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
        z = pd.read_sql_table(table_name, conn, schema=self.dbschema, index_col=None, coerce_float=True,
                              parse_dates=None, columns=None, chunksize=rows)
        return z

    def put_pandas_frame(self, table_name, df):
        conn, meta = self.connect_sqlalchemy()
        z = df.to_sql(table_name, conn, schema=self.dbschema, index=False, if_exists='append', chunksize=None,
                      dtype=None)
        return z
