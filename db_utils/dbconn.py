import logging as lg
import os
# import subprocess as commands
import commands
import sys
import datetime

import migrate_utils
lg.basicConfig()
logging = lg.getLogger()
# logging.setLevel(lg.INFO)
logging.setLevel(lg.DEBUG)

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
    # _sqlalchemy_con = None this needs to be instance
    # _sqlalchemy_meta = None this needs to be instance

    def __init__(self, dbschema=None, commit=True, password=None, userid=None, host=None, port=5432, database=None,
                 dbtype='POSTGRES', appname='py_dbutils'):
        """ Default to commit after every transaction
        """

        self._commit = commit
        self.dbschema = dbschema
        self.appname = appname

        self._database_name = database

        self._port = port

        self._userid = userid

        self._password = password

        self._host = host
        self._sslmode = None

        logging.debug("DB Connecting To: {0}:{1}:{2}".format(self._host, self._database_name, dbtype))
        self._dbtype = dbtype.upper()
        if self._dbtype in ['POSTGRES','CITUS']:
            self._conn = self._connect_postgres
        if self._dbtype == 'MSSQL':
            self._conn = self._connect_mssql()
        if self._dbtype == 'MYSQL':
            self._conn = self._connect_mysql()
        if self._dbtype == 'ORACLE':
            self._conn = self._connect_oracle()
        self._cur = self._conn.cursor()
        self.url = None  # db url
        logging.debug("DB Connected To: {0}:{1}:{2}:{3}".format(self._host, self._database_name, dbtype, self._userid))

        self._sqlalchemy_con = None
        self._sqlalchemy_meta = {}

    # @migrate_utils.static_func.timer
    def connect_sqlalchemy(self, schema=None, db_type=None):
        import sqlalchemy
        # import pymssql
        '''Returns a connection and a metadata object'''
        # We connect with the help of the PostgreSQL URL
        # postgresql://federer:grandestslam@localhost:5432/tennis

        if db_type is None and self._dbtype is None:
            db_type = "POSTGRES"
        else:
            db_type = self._dbtype
        if schema is None:
            schema = self.dbschema

        if self._sqlalchemy_con is None or self._sqlalchemy_meta.get(schema, None) is None:

            if db_type.upper() == "POSTGRES":
                self.url = 'postgresql://{}:{}@{}:{}/{}'
                self.url = self.url.format(self._userid, self._password, self._host, self._port, self._database_name)

                if self._sqlalchemy_con is None:
                    self._sqlalchemy_con = sqlalchemy.create_engine(self.url, client_encoding="utf-8",
                                                                    connect_args={"application_name": self.appname})
            if db_type.upper() == "MSSQL":
                self.url = 'mssql+pymssql://{}:{}@{}:{}/{}'
                self.url = self.url.format(self._userid, self._password, self._host, self._port, self._database_name)
                self._sqlalchemy_con = sqlalchemy.create_engine(self.url)
            if db_type.upper() == "MYSQL":
                # 'mysql+pymysql://root:test@192.168.99.100:3306/mysql'
                self.url = "mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8"
                self.url = self.url.format(self._userid, self._password, self._host, self._port, self._database_name)
                self._sqlalchemy_con = sqlalchemy.create_engine(self.url)

            # con=self._connect_mssql()
            # The return value of create_engine() is our connection object

            # We then bind the connection to MetaData()
            # print('connecting schema:', schema)
            if self._sqlalchemy_meta.get(schema, None) is None:
                self._sqlalchemy_meta[schema] = sqlalchemy.MetaData(bind=self._sqlalchemy_con, reflect=True,
                                                                    schema=schema)

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

    def get_schema_col_stats(self, schema=None):
        v_schema = self.dbschema if schema is None else schema
        sql = """select  schemaname,tablename,attname as columnname,n_distinct , reltuples::bigint
            ,( (select   distinct(True)  from pg_index ix,pg_attribute ab where  ab.attnum = ANY(ix.indkey)
            and ab.attrelid = a.oid
            and ix.indrelid=a.oid 
            and ab.attname=p.attname    )   )as is_index
            from pg_stats p,
            pg_class a
            where 
            a.oid=concat(schemaname,'.',tablename)::regclass and 
            a.relkind='r' and 
            a.relname=tablename and
            schemaname='{}' and 
            n_distinct>0 order by 5,4;
            """.format(v_schema)
        return self.query(sql)

    def get_schema_index(self, schema=None):
        import pprint

        v_schema = self.dbschema if schema is None else schema

        return (self.query("""select
    ns.nspname,
    t.relname as table_name,
    i.relname as index_name,
    array_to_string(array_agg(  a.attname), ', ') as column_names,
    ix.indisprimary as is_primarykey
from
    pg_namespace ns,
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a
where
    ns.oid=t.relnamespace
    and t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    -- and t.relname like 'test%'
    and nspname='{}'
group by
    t.relname,
    i.relname,
    ns.nspname,

    ix.indisprimary
order by
    t.relname,
    i.relname;""".format(v_schema)))
    def get_create_table_sqlalchemy(self, table_name,trg_db):
        import sqlalchemy
        stmt=None
        con, meta = self.connect_sqlalchemy()
        t=table_name.split('.')[-1]
      
        table = sqlalchemy.Table(t, meta, autoload=True, autoload_with=con)
        print("xxxxx ",type(table))
        trg_con,trg_meta=trg_db.connect_sqlalchemy()
        stmt = sqlalchemy.schema.CreateTable(table)
       
        print(stmt,'xxx')
            
        print(str(stmt).strip())
        return stmt
        
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
                logging.ERROR("Cannot Find Table: {}".format(t))

    def get_create_table(self, table_name):
        sql = """SELECT                                          
  'CREATE TABLE ' || relname || E'\n(\n' ||
  array_to_string(
    array_agg(
      '    ' || column_name || ' ' ||  type || ' '|| not_null
    )
    , E',\n'
  ) || E'\n);\n'
from
(
  SELECT 
    c.relname, a.attname AS column_name,
    pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
    case 
      when a.attnotnull
    then 'NOT NULL' 
    else 'NULL' 
    END as not_null 
  FROM pg_class c,
   pg_attribute a,
   pg_type t
   WHERE c.relname = {table_name}
   AND a.attnum > 0
   AND a.attrelid = c.oid
   AND a.atttypid = t.oid
 ORDER BY a.attnum
) as tabledefinition
group by relname;""".format(table_name)
        create_sql = self.get_a_value(sql)
        return create_sql

    def get_create_table_via_dump(self, table_name, target_name=None, gen_pk=True, gen_index=True, gen_fk=True):
        v_source_schema = table_name.split('.')[0]
        v_target_schema = target_name.split('.')[0]
        if target_name is None:
            target_name = table_name

        self._save_environment_vars()
        self._replace_environment_vars()

        cli = """pg_dump -st {} {}""".format(table_name, v_source_schema)

        sql = ''

        bash_error_code, txt_out = commands.getstatusoutput(cli)
        lines = txt_out.split('\n')
        line_list = []
        logging.debug(txt_out)
        if len(lines) < 3:
            raise Exception(txt_out)
        for i, line in enumerate(lines):
            if not (line.startswith('--') or line == ''):
                line_list.append(line)
        lines2 = '\n'.join(line_list)
        lines2 = lines2.split(';\n')
        for i, line in enumerate(lines2):
            print(i, line)

        self._restore_environment_vars()
        if v_index_sql == '':
            v_index_sql = None
        return sql, v_index_sql

    def get_create_table_cli(self, table_name, target_name=None, gen_pk=True, gen_index=True, gen_fk=True):
        v_source_schema = table_name.split('.')[0]
        v_target_schema = target_name.split('.')[0]
        if target_name is None:
            target_name = table_name

        self._save_environment_vars()
        self._replace_environment_vars()

        cli = """psql -c"\d {}" """.format(table_name)
        #cli = """psql {6} -c "\copy {0} FROM '{1}' with (format csv,{4} FORCE_NULL ({3}),delimiter '{2}', ENCODING '{5}')" """

        sql = ''
        v_index_sql = ''
        bash_error_code, txt_out = commands.getstatusoutput(cli)
        lines = txt_out.split('\n')
        v_index = False
        logging.debug(txt_out)
        if len(lines) < 3:
            raise Exception(txt_out)
        for i, line in enumerate(lines):
            if i == 0:
                sql += "Create table {}(".format(target_name)
            elif (line == 'Indexes:' or line == 'Foreign-key constraints:' or line.startswith('Options:') or
                  line.startswith('Referenced by:')):
                if not v_index:
                    sql += '); '
                v_index = True
            elif i > 2 and not v_index and len(line) > 1:
                x = line.split('|')
                v_default = ''
                if len(x[3]) > 1:
                    v_default = ' ' + x[3]
                if i == 3:
                    sql += '\n' + x[0] + ' ' + x[1] + ' ' + x[2]
                else:
                    sql += ',\n' + x[0] + ' ' + x[1] + ' ' + x[2]
                sql += v_default

            elif v_index and len(line) > 1 and 'PRIMARY KEY' in line:
                if gen_pk:
                    v_index_sql += 'ALTER TABLE {0} ADD PRIMARY KEY ('.format(target_name)
                    v_index_sql += line.split('(')[1] + ';\n'

            elif v_index and len(line) > 1 and 'FOREIGN KEY' in line:
                if gen_fk:

                    if 'REFERENCES' in line and line.startswith('TABLE '):
                        v_referenced_table = line.split('"')[1]
                        v_index_sql += 'ALTER TABLE {0} ADD CONSTRAINT FOREIGN KEY ('.format(v_referenced_table)
                        v_index_sql += str(line.split('(')[1] + ';\n').replace(v_source_schema + '.', v_target_schema + '.')
                    else:
                        v_index_sql += 'ALTER TABLE {0} ADD CONSTRAINT FOREIGN KEY ('.format(target_name)
                        v_index_sql += str(line.split('(')[1] + ';\n').replace(v_source_schema + '.', v_target_schema + '.')
            elif v_index and len(line) > 1 and 'UNIQUE' in line:
                if gen_index:
                    v_index_sql += 'CREATE UNIQUE INDEX  ON {0} ('.format(target_name)
                    v_index_sql += str(line.split('(')[1] + ';\n')
            elif v_index and len(line) > 1 and 'btree' in line:
                if gen_index:
                    v_index_sql += 'CREATE INDEX  ON {0} '.format(target_name)
                    v_index_sql += str(line.split('btree')[1] + ';\n')
            elif line == 'Triggers:':
                break
            else:
                if len(line) > 5 and i > 3:
                    raise Exception(line)
                logging.debug("get_create_table_cli - Unknown Line: {0}:{1}".format(i, line))

            # if no index where ever found close the bracket
        if v_index is False:
            sql += ');'

        self._restore_environment_vars()
        if v_index_sql == '':
            v_index_sql = None
        return sql, v_index_sql

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

    def schema_exists(self, schema_name):
        v_found = False

        v_found = self.has_record(
            """select 1 from information_schema.schemata where schema_name='{0}' limit 1""".format(schema_name))

        return v_found

    def table_exists(self, table_name):
        v_table_exists = False
        v_schema = table_name.split('.')[0]
        v_table_name = table_name.split('.')[1]
        v_table_exists = self.has_record(
            """select 1 from information_schema.tables where table_schema='{0}' and table_name='{1}'""".format(v_schema, v_table_name))

        return v_table_exists

    def has_record(self, sqlstring):
        rs = None
        try:
            rs = self.query(sqlstring)
        except Exception as e:
            logging.error("error in dbconn.has_record: {}".format(sqlstring))

        if len(rs) > 0:
            return True
        return False

    def query(self, sqlstring):
        """ runs query or procedure that returns record set
        """
        logging.debug('Running Query: {}\n\t{}'.format(datetime.datetime.now().time(), sqlstring))
        if sqlstring.lower().startswith('select') or sqlstring.lower().startswith('call'):
            self._cur.execute(sqlstring)
            rows = self._cur.fetchall()
        else:
            raise Exception('Only Selects allowed')
        logging.debug('Query Completed: {}'.format(datetime.datetime.now().time()))
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
        self._cur.execute("COMMIT")

    def rollback(self):
        self._cur.execute("ROLLBACK")

    def vacuum(self, dbschema=None, table_name=None):
        """ Default to commit after every transaction
                Will check instance variable to decide if a commit is needed
        """

        self.commit()
        if table_name is None or table_name == '':
            self._cur.execute("vacuum analyze")
            logging.debug("Vacuuming Schema")
        elif '.' in table_name:
            logging.debug("Vacuuming Table:{0}".format(table_name))
            self._cur.execute("vacuum {0}".format(table_name))
        else:
            logging.debug("Vacuuming Table:{0}".format(table_name))
            self._cur.execute("vacuum {0}.{1}".format(dbschema, table_name))

    def execute(self, sqlstring, debug=False):

        logging.debug("Debug DB Execute: {}:{}:{} \n\t{} ".format(self._userid, self._host, self._database_name, sqlstring))
        rowcount = 0
        try:
            self._cur.execute(str(sqlstring).strip())
            rowcount = self._cur.rowcount
            self.commit()
        except Exception as e:
            #print("Error Execute SQL:{}".format(e))
            logging.warning("SQL error Occurred But Continuing:\n{}".format(e))
            import time
            time.sleep(2)

        logging.debug("DB Execute Completed: {}:{}:{}".format(self._userid, self._host, self._database_name))

        return rowcount

    def execute_permit_execption(self, sqlstring, debug=False):
        # cloning previous method to avoid breaking things already in place
        logging.debug("Debug DB Execute: {}:{}:{} \n\t{} ".format(self._userid, self._host, self._database_name, sqlstring))
        rowcount = 0
        
        self._cur.execute(str(sqlstring).strip())
        rowcount = self._cur.rowcount
        self.commit()

        return rowcount

    def drop_schema(self, schema):
        logging.debug(
            "Drop Database Schema: \n\tHost:{0}\n\tDatabase:{1}\nSchema:{2}".format(self._host, self._database_name,
                                                                                    schema))
        print(self._cur.execute('drop schema {0} cascade'.format(schema)))

        self.commit()

    def truncate_table(self, dbschema, table_name):
        logging.debug(
            "Truncating Table: \n\tHost:{0}\n\tDatabase:{1}\n\tTablename:{2}\n\tSchema:{3}".format(self._host, self._database_name,
                                                                                                   table_name, dbschema))
        self._cur.execute('TRUNCATE table {0}.{1} cascade'.format(dbschema, table_name))

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
                self._sslmode = os.getenv('PGSSLMODE', 'prefer')
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
                                host=self._host, application_name=self.appname,sslmode='prefer')
        conn.set_client_encoding('UNICODE')
        logging.debug('Connected to POSTGRES: {}:{}:{}'.format(self._host, self._database_name, self._userid))
        return conn

    def _connect_mssql(self ):
        import pymssql

        self._password = os.environ.get('MSPASSWORD',None)

        

        # conn = pymssql.connect(server=self._host, user=self._userid, password=self._password,
        #                       database=self._database_name, host=self._host, port=self._port, conn_properties=None,
        #                       timeout=0, login_timeout=60, charset='UTF-8', as_dict=False, appname=appname,
        #                       autocommit=self._commit, tds_version='7.1')
        print(
             self._host,
             self._port,
             self._userid,
             self._password,
             self._database_name)
        
        conn = pymssql.connect(
            server=self._host,
            port=self._port,
            user=self._userid,
            password=self._password,
            database=self._database_name)
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
        total_rows=0
        if self._dbtype in ['POSTGRES','CITUS']:
        
            # save the Current shell password
            self._save_environment_vars()
            self._replace_environment_vars()
            # copy_command_client_side = """psql -c "\copy {0} FROM '{1}' with (format csv, delimiter '{2}')" """
            shell_command = """psql -c "\copy ({0}) to '{1}' WITH DELIMITER AS '{2}' CSV QUOTE AS '\\"' " """
    
            command_text = (shell_command.format(sqlstring, full_file_path, delimiter))
    
            prev_password = os.environ['PGPASSWORD']
    
            logging.info("Dumping Data to CSV STARTED:{0}".format(full_file_path))
            logging.debug("SQL:{0}".format(sqlstring))
            logging.debug("Command string: \nt\t{}".format(command_text))
            error_code, txt_out = commands.getstatusoutput(command_text)
            logging.debug("Dumping Data to CSV COMPLETED:{0}".format(txt_out))
            self._restore_environment_vars()
            if error_code > 0:
                raise Exception
            i = txt_out.split()
            logging.info("Total Rows Dumped: {0}".format(i[1]))
            total_rows=int(i[1])
        if  self._dbtype in ['MSSQL']:
            import pandas
             
            df=pandas.read_sql_query(sqlstring, self._conn, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=100000)
            #df.to_csv(full_file_path,header=false,sep=delimiter)
           
            for chunk in df:
                total_rows+=len(chunk)
                chunk.to_csv(full_file_path,header=False,sep=delimiter,index=False, mode='a')
            
        return total_rows

    def get_conn_url(self):

        return self.url

    # @migrate_utils.static_func.timer
    def get_table_list_via_query(self, dbschema):
        sql = """SELECT table_name FROM information_schema.tables a
            WHERE table_schema='{}' and table_type='BASE TABLE'""".format(dbschema)
        result_set = self.query(sql)
        return [r[0] for r in result_set]

    def get_view_list_via_query(self, dbschema):
        sql = """SELECT table_name FROM information_schema.tables a
            WHERE table_schema='{}' and table_type='VIEW'""".format(dbschema)
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
            class data:
                pass

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
        from sqlalchemy.ext.automap import automap_base
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

    # @migrate_utils.static_func.timer
    def get_columns(self, table_name, table_schema):
        """

        :rtype: object
        """
        # type: (str, str) -> list
        import sqlalchemy
        try:
            print("Getting Column List from DB: {}.{}".format(table_schema, table_name))
            con, meta = self.connect_sqlalchemy(table_schema, self._dbtype)
            table = sqlalchemy.Table(table_name, meta, autoload=True, autoload_with=con)
            column_list = [c.name for c in table.columns]
            return list(column_list)
        except:
            logging.warning("No Columns found when trying to get Column List: Returning None")
            return []

    # returns a list of table dict
    # @migrate_utils.static_func.timer
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
        if self.dbtype in ['POSTGRES','CITUS']:
            self.vacuum(table_name)
            row = self.query("""select n_live_tup
                    from pg_stat_user_tables
                    where schemaname='{}' and relname='{}'""".format(schema, table_name))
            x = row[0][0]

        return x

    def get_a_value(self, sql):

        x = self.query(sql)
        value = x[0][0]

        return value

    def get_tables_row_count(self, schema=None):
        if schema is None:
            schema = self.dbschema
        con, meta = self.connect_sqlalchemy(schema, self._dbtype)
        # print dir(meta.tables)

        table_obj = []
        for n, t in meta.tables.items():
            # print(n, t.name)

            # print(type(n), n, dir(t))
            x = self.query("select count(1) from {}".format(t.key))
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
        for row in result:
            # convert tuple to variable when having atleast 2 columns
            field_name, data_type = row
            field_list.append(field_name)
        return field_list

    def print_table_info(self, table_name, dbschema):
        from sqlalchemy.ext.automap import automap_base
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
    # certain commans requires the environment variables for the session
    # we need to save that and replace with our current and put it back after we are done

    def _save_environment_vars(self):
        if self._dbtype in ['POSTGRES','CITUS']:
            self._pg_password = os.environ.get('PGPASSWORD',self._password)
            self._pg_userid = os.environ.get('PGUSER',self._userid)
            self._pg_sslmode = os.environ.get('PGSSLMODE',self._sslmode)
            self._pg_host = os.environ.get('PGHOST',self._host)
            self._pg_port = os.environ.get('PGPORT',str(self._port))
            self._pg_database_name = os.environ.get('PGDATABASE',self._database_name)

    def _restore_environment_vars(self):
        if self._dbtype in ['POSTGRES','CITUS']:
            os.environ['PGPASSWORD'] = self._pg_password
            os.environ['PGUSER'] = self._pg_userid
            os.environ['PGSSLMODE'] = self._pg_sslmode
            os.environ['PGHOST'] = self._pg_host
            os.environ['PGPORT'] = self._pg_port
            os.environ['PGDATABASE'] = self._pg_database_name

    def _replace_environment_vars(self):
        if self._dbtype in ['POSTGRES','CITUS']:
            os.environ['PGPASSWORD'] = self._password
            os.environ['PGUSER'] = self._userid
            os.environ['PGSSLMODE'] = self._sslmode
            os.environ['PGHOST'] = self._host
            os.environ['PGPORT'] = str(self._port)
            os.environ['PGDATABASE'] = self._database_name

    def import_pyscopg2_copy(self, full_file_path, table_name_fqn, file_delimiter):
   
        f = open(full_file_path)
        #cur.copy_from(f, table_name_fqn, columns=('col1', 'col2'), sep=",")
        x=self._cur.copy_from(f, table_name_fqn,  sep=",")
        self._conn.commit()
        print("-----xxxx pyscop copy: ",x)
        return x
    # simple import using client side
    # this assumes the csv has data exactly in the same structure as the target table

    def import_file_client_side(self, full_file_path, table_name_fqn, file_delimiter):

        self._save_environment_vars()
        self._replace_environment_vars()
        copy_command_client_side = """psql --dbname={3} --host={4} -c "\copy {0} FROM '{1}' with (format csv, delimiter '{2}')" """

        data_file = full_file_path

        t = datetime.datetime.now()

        command_text = copy_command_client_side.format(table_name_fqn, data_file, file_delimiter, self._database_name,
                                                       self._host)
        logging.info("Copy Command STARTED:{0} Time:{1}".format(table_name_fqn, t))
        error_code, txt_out = commands.getstatusoutput(command_text)
        logging.debug("Copy Command Completed:{0} Time:{1}".format(txt_out, datetime.datetime.now()))
        logging.debug("Total Time:{0} ".format(datetime.datetime.now() - t))
        self._restore_environment_vars()
        #logging.error("Inserted :{}".format(data_file))
        if error_code > 0:
            logging.error(txt_out)
            logging.error(data_file)
            logging.error(error_code)
            raise Exception

        i = txt_out.split()
        logging.info("Total Rows Loaded: {0}".format(i[1]))

        return i[1]
