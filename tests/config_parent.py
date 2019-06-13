 
import inspect
import os, logging as log
runtime_pid=os.getpid()
logging = log.getLogger(f'\tPID: {runtime_pid} - {os.path.basename(__file__)}\t')
logging.setLevel(log.DEBUG)
class Config(object):

    TEST_SCHEMA = 'test'
    LOGGING_SCHEMA = 'logging'
    PROJECT_NAME= 'test_project'

    def whoami(self): 
        return inspect.getouterframes(inspect.currentframe())[1].function

    dirs = {
        'data_dir': "/workspace/_sample_data/",
        'sample_data_dir': "/workspace/_sample_data/",
        'working_dir': "/workspace/_sample_working_dir/",
        'sample_working_dir': "/workspace/_sample_working_dir/",
        'sample_zip_data_dir': "/workspace/_sample_zip_data/",
        'zip_data_dir': "/workspace/_sample_zip_data/"}
    
    def get_pg_database(self,appname=__file__):
        from py_dbutils.rdbms import postgres as db_utils
        HOST = os.environ['PGHOST'] or 'localhost'
        DATABASE = os.environ['PGDATABASE'] or 'postgres'
        DBPORT= os.environ['PGPORT'] or 5432
        USERID = os.environ['PGUSER'] or 'docker' 
        DBPASSWORD = os.environ['PGPASSWORD'] or 'docker'

        db = db_utils.DB(host=HOST, userid=USERID, dbname=DATABASE, schema=self.TEST_SCHEMA,
                                    pwd=DBPASSWORD,  port=DBPORT, label=appname)
        db.execute("create schema {}".format(self.LOGGING_SCHEMA))
        db.execute("drop schema {} cascade".format(self.TEST_SCHEMA))
        db.execute("create schema {}".format(self.TEST_SCHEMA))
        db.execute("create schema {}".format(self.TEST_SCHEMA))
        sql_files=[]
        sql_files.append('/workspace/tests/sql/create_schema.sql')
        sql_files.append('/workspace/tests/sql/test_function.sql')
        sql_files.append('/workspace/tests/sql/test_view.sql')
        sql_files.append('/workspace/tests/sql/logging_tables.sql')
        #with open(vw_file,'r') as file:
        #    data = file.read().replace('\n', '')
        db.create_cur()
        for f in sql_files:
            with open(f,'r') as sql_file:
                db.cursor.execute(sql_file.read())
        db.commit()
        return db