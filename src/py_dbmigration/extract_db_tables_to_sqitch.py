#!python

# import csv, pandas,        sqlalchemy, os
import os
import db_utils.dbconn as db_utils

import migrate_utils.static_func as migu

import logging as lg
lg.basicConfig()
logging = lg.getLogger()
logging.setLevel(lg.INFO)


pghost = os.environ['PGHOST']
pgdatabase = os.environ['PGDATABASE']
"""
    Creating a Database connection object
"""


# sqlserver = db_utils.Connection(dbschema='dbo', database='enforce', dbtype='MSSQL', host=host, commit=False)
db_postgres = db_utils.Connection(dbschema=logging, database=pgdatabase,
                                  dbtype='POSTGRES', host=pghost, commit=True)

schemas = get_schema_except(db_postgres, ['op_dba', 'public', 'pg_catalog', 'information_schema', 'citus', 'sys', 'sqitch'])
for s in schemas:
    db_postgres = db_utils.Connection(dbschema=s, database=pgdatabase,
                                      dbtype='POSTGRES', host=pghost, commit=True)
    migu.change_table_owner(db_postgres, s, 'operational_dba')
    migu.change_view_owner(db_postgres, s, 'operational_dba')

    migu.print_create_table(db_postgres, folder="~/_{}/{}/".format(pgdatabase, s),
                            targetschema=s, file_prefix='{}.'.format(s))
    migu.print_create_functions(db_postgres, folder="../../../_{}/{}/".format(pgdatabase, s),
                                targetschema=s, file_prefix='{}.'.format(s))
    migu.print_create_views(db_postgres, folder="../../../_{}/{}/".format(pgdatabase, s),
                            targetschema=s, file_prefix='{}.'.format(s))
