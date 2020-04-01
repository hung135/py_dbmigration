 
import inspect
import os, logging as lg 

logging = lg.getLogger()

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
    
    def get_yaml_schemas(self,yaml_file,loglevel=None):
        from yaml import Loader
        import yaml

        db=self.get_pg_database(loglevel=loglevel)
        with open(yaml_file,'r') as f:
            yaml_data = yaml.full_load(f)
            print(yaml_file)
            for idx in yaml_data:
                for mapping in idx.get('mapping',[]):
                    for test_schema in mapping:
 
                        if test_schema=='schema_name': 
                            db.execute("create schema {}".format(test_schema))
            
        return 1

    def get_pg_database(self,appname=__file__,loglevel=None):
         
        from py_dbutils.rdbms import postgres as db_utils
        HOST = os.environ['PGHOST'] or 'localhost'
        DATABASE = os.environ['PGDATABASE'] or 'postgres'
        DBPORT= os.environ['PGPORT'] or 5432
        USERID = os.environ['PGUSER'] or 'docker' 
        DBPASSWORD = os.environ['PGPASSWORD'] or 'docker'

        db = db_utils.DB(host=HOST, userid=USERID, dbname=DATABASE, schema=self.TEST_SCHEMA,
                                    pwd=DBPASSWORD,  port=DBPORT, label=appname,loglevel=loglevel)
        db.execute("create schema {}".format(self.LOGGING_SCHEMA))
        db.execute("drop schema {} cascade".format(self.TEST_SCHEMA))
        db.execute("create schema {}".format(self.TEST_SCHEMA))
        
        sql_files=[]
        sql_files.append('/workspace/tests/sql/create_schema.sql')
        sql_files.append('/workspace/tests/sql/create_switchboard.sql')
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