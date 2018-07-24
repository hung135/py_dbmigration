#!python

# import csv, pandas,        sqlalchemy, os
import os
import db_utils.dbconn as db_utils
import data_file_mgnt as dfm
import migrate_utils.static_func as migu

import logging as lg
lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)

file_path = os.environ['RAWFILEPATH']
writable_path = os.environ['WORKINGPATH']
host = os.environ['MSSQLHOST']
pghost = os.environ['PGHOST']
pgdatabase = os.environ['PGDATABASE']
"""
    Creating a Database connection object
"""


# sqlserver = db_utils.Connection(dbschema='dbo', database='enforce', dbtype='MSSQL', host=host, commit=False)

schemas = [pgdatabase, 'stg', 'logging']
for s in schemas:
    db_postgres = db_utils.Connection(dbschema=s, database=pgdatabase,
                                      dbtype='POSTGRES', host=pghost, commit=False)
    migu.change_table_owner(db_postgres, s, 'operational_dba')
    migu.change_view_owner(db_postgres, s, 'operational_dba')

    migu.print_create_table(db_postgres, folder="/home/cfpb/nguyenhu/_{}/{}/".format(pgdatabase, s), targetschema=s, file_prefix='{}.'.format(s))
