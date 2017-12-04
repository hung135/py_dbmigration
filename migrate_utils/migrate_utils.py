import logging


def print_sqitch_files(folder, file_type, trg_folder):
    import os
    from os import listdir
    from os.path import isfile, join, basename

    onlyfiles = [f for f in listdir(folder) if isfile(join(folder, f))]
    for ff in onlyfiles:
        if ff.endswith(file_type):
            filename = os.path.splitext(ff)[0]
            print("sqitch add {}/{} -n \"Adding {}\" ".format(trg_folder, filename, ff))

# pass in the string and a dict of key to value mapping
# we will replace all the keys will the mapped value found in the string
# not a perfect implementation but good for autogenerating some scripts


def convert_sql_snake_case(string, column_list):
    import inflection
    string = string.replace('COLLATE \"SQL_Latin1_General_CP1_CI_AS\"', "")
    string = string.replace(') ,', "),")
    for i in column_list:
        newfield = inflection.underscore(i)
        newfield = newfield.replace(" ", "_")
        newfield = newfield.replace("(", "_")
        newfield = newfield.replace(")", "_")
        newfield = newfield.replace('COLLATE \"SQL_Latin1_General_CP1_CI_AS\"', "")

        string = string.replace(i, newfield)

    return string.lower()
def convert_list_to_snake_case(column_list):
    import inflection
   
    newlist=[]
    for i in column_list:
        newfield = inflection.underscore(i)
        newfield = newfield.replace("   ", " ")
        newfield = newfield.replace("  ", " ")
        newfield = newfield.replace(" ", "_")
        newfield = newfield.replace("\\", "_")
        newfield = newfield.replace("/", "_")
        newfield = newfield.replace("'", "_")
        newfield = newfield.replace("(", "_")
        newfield = newfield.replace(")", "_")
        newfield = newfield.replace(")", "_")
        newfield = newfield.lower()
        newlist.append(newfield)
    return newlist
def make_markdown_table(array):

    """ 
    Stolen from here:
    https://gist.github.com/m0neysha/219bad4b02d2008e0154#file-pylist-to-markdown-py
    Input: Python list with rows of table as lists
               First element as header. 
        Output: String to put into a .md file 
        
    Ex Input: 
        [["Name", "Age", "Height"],
         ["Jake", 20, 5'10],
         ["Mary", 21, 5'7]] 
    """


    markdown = "\n" + str("| ")

    for e in array[0]:
        to_add = " " + str(e) + str(" |")
        markdown += to_add
    markdown += "\n"

    markdown += '|'
    for i in range(len(array[0])):
        markdown += str("-------------- | ")
    markdown += "\n"

    for entry in array[1:]:
        markdown += str("| ")
        for e in entry:
            to_add = str(e) + str(" | ")
            markdown += to_add
        markdown += "\n"

    return markdown + "\n"

def show_users(db):

    sql="""select usename
        -- ,rolname 
        from pg_user
        join pg_auth_members on (pg_user.usesysid=pg_auth_members.member)
        join pg_roles on (pg_roles.oid=pg_auth_members.roleid)
        where rolname='{}_readonly'
        or rolname='{}_readonly
        ;""".format(db._database_name,db.dbschema)
    return db.query(sql)


def appdend_to_readme(db, folder=None, targetschema=None):
    with open("../README.md") as f:
        content = f.readlines()
    dictionary="<a name=\"data_dictionary\"></a>"[:25]
    dict_query="""select table_schema,table_name,column_name,data_type,character_maximum_length
                from information_schema.columns a 
                where table_schema='enforce' order by table_name,ordinal_position"""
    header=["table_schema"+"|"+"table_name"+"|"+"column_name"+"|"+"data_type"+"|","length"]
    header=["table_schema","table_name","old_column_name","column_name","data_type","length"]
    pad_size=30
    table=[header]
    with open(folder+"/README.md","wb") as f:
        for line in content:
            f.write(bytes(line,'UTF-8'))
            if line[:25]==dictionary[:25]:
                #f.write(bytes(header,'UTF-8'))
                rows=db.query(dict_query)
                for row in rows:
                    table_schema=""
                    table_name=""
                    column_name=""
                    data_type=""
                    length=""

                    table_schema=row[0]
                    table_name=row[1].rjust(pad_size," ")
                    column_name=row[2].rjust(pad_size," ")
                    data_type=row[3].rjust(pad_size," ")
                    if row[4] is not None:
                        length=str(row[4])
                    length=length.rjust(6," ")
                    #table.append([table_schema,table_name,column_name,data_type,length])
                    #table.append(row)
                    table.append([table_schema,table_name,column_name,column_name,data_type,length])
                    #line=table_schema+"|"+table_name+"|"+old_column_name+"|"+column_name+"|"+data_type+"|"+length+"\n"


                f.write(bytes(make_markdown_table(table),'UTF-8'))
        #print(make_markdown_table(table))



def print_postgres_table(db, folder=None, targetschema=None):
    import migrate_utils as mig
    import sqlalchemy
    import os
    from sqlalchemy.dialects import postgresql
    import subprocess 
    
    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_table = folder + "/postgrestables/"

    os.makedirs(folder_table, exist_ok=True)
 

    sqitch = []
    table_count = 0
    # for n, t in meta.tables.iteritems():
    for n, t in meta.tables.items():
        table_count += 1
        filename = t.name.lower() + ".sql"
        basefilename = t.name.lower()
        out=None
        try:
            out=subprocess.check_output(["pg_dump","--schema-only","enforce","-t","{}.{}".format(db.dbschema, t.name)])
    #"pg_dump -U nguyenhu enforce -t  public.temp_fl_enforcement_matters_rpt --schema-only"
            #print(out)
        except subprocess.CalledProcessError as e:
            print(e)
        
        logging.debug("Generating Postgres Syntax Table: {}".format(t.name.lower()))
        

        with open(folder_table + filename, "wb") as f:
            f.write(out)    
            f.write(bytes("\n", 'UTF-8'))

    print("Total Tables:{}".format(table_count))

def print_create_table_upsert(db, folder=None, targetschema=None):
    import migrate_utils as mig
    import sqlalchemy
    import os
    from sqlalchemy.dialects import postgresql

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_deploy = folder + "/deploy/functions/"
    folder_revert = folder + "/revert/functions/"
    folder_verify = folder + "/verify/functions/"
    os.makedirs(folder_deploy, exist_ok=True)
    os.makedirs(folder_revert, exist_ok=True)
    os.makedirs(folder_verify, exist_ok=True)

    sqitch = []
    table_count = 0
    # for n, t in meta.tables.iteritems():
    for n, t in meta.tables.items():
        table_count += 1
        filename = t.name.lower() + "_upsert.sql"
        basefilename = t.name.lower()
        rows = db.query("call {}.generateUpsert_style_functions('{}','{}')".format(db._database_name, db.dbschema, t.name))
        logging.debug("Generating Upsert for Table: {}".format(t.name.lower()))
        line = ("\nsqitch add functions/{} -n \"Adding {}\" ".format(basefilename + "_upsert", filename))
        sqitch.append(line)

        with open(folder_deploy + filename, "wb") as f:
            for line in rows:
                f.write(bytes(line[0], 'UTF-8'))
                f.write(bytes("\n", 'UTF-8'))

        drop = "DROP FUNCTION IF EXISTS {}.{};".format(db.dbschema, basefilename + "_upsert();")
        with open(folder_revert + filename, "wb") as f:
            f.write(bytes(drop, 'UTF-8'))
            f.write(bytes("\n", 'UTF-8'))

        with open(folder_verify + filename, "wb") as f:
            f.write(bytes("-- NA ", 'UTF-8'))
            f.write(bytes("\n", 'UTF-8'))
    print("Total Tables:{}".format(table_count))
    with open(folder + "/sqitchplanadd_upsert.bash", "wb") as f:
        f.write(bytes("# This is Auto Generated from migrate_utils.py print_create_table_upsert()", 'UTF-8'))
    for s in sqitch:
        with open(folder + "/sqitchplanadd_upsert.bash", "a") as f:
            f.write(s)


def print_table_dict(db, folder=None, targetschema=None):
    import migrate_utils as mig
    import sqlalchemy
    import os
    from sqlalchemy.dialects import postgresql

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_dict = folder + "/dict/"

    table_count = 0

    tables = []

    # for n, t in meta.tables.iteritems():
    for n, t in meta.tables.items():
        table_count += 1
        filename = t.name.lower() + ".sql"
        basefilename = t.name.lower()
        #print(type(n), n, t.name)
        table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
        stmt = sqlalchemy.schema.CreateTable(table)
        column_list = [c.name for c in table.columns]
        createsql = mig.convert_sql_snake_case(str(stmt), column_list)

        logging.debug("Generating Create Statement for Table: {}".format(t.name.lower()))
        line = ("\nsqitch add tables/{} -n \"Adding {}\" ".format(basefilename, filename))

        sqitch.append(line)
        if targetschema is not None:
            createsql = createsql.replace(("table " + db.dbschema + ".").lower(), "table " + targetschema + ".")
        m = {"table": basefilename, "sql": createsql + ";\n", "filename": filename}
        tables.append(m)

    print("Total Tables:{}".format(table_count))


def print_create_table(db, folder=None, targetschema=None,file_prefix=None):
    import migrate_utils as mig
    import sqlalchemy
    import os
    from sqlalchemy.dialects import postgresql

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_deploy = folder + "/deploy/tables/"
    folder_revert = folder + "/revert/tables/"
    folder_verify = folder + "/verify/tables/"
    os.makedirs(folder_deploy, exist_ok=True)
    os.makedirs(folder_revert, exist_ok=True)
    os.makedirs(folder_verify, exist_ok=True)
    table_count = 0
    sqitch = []
    tables = []
    
    if targetschema is None:
        dbschema=db.dbschema

    else:
        dbschema=db.dbschema


    # for n, t in meta.tables.iteritems():
    for n, t in meta.tables.items():
        table_count += 1
 
        if file_prefix is not None:
            filename = file_prefix+t.name.lower() + ".sql"
        else:
            filename = t.name.lower() + ".sql" 
        basefilename = t.name.lower()
        #print(type(n), n, t.name)
        table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
        stmt = sqlalchemy.schema.CreateTable(table)
        column_list = [c.name for c in table.columns]
        createsql = mig.convert_sql_snake_case(str(stmt), column_list)
        logging.debug("Generating Create Statement for Table: {}".format(t.name.lower()))
        line = ("\nsqitch add tables/{} -n \"Adding {}\" ".format(basefilename, filename))

        sqitch.append(line)
        if targetschema is not None:
            createsql = createsql.replace(("table " + dbschema + ".").lower(), "table " + targetschema + ".")
        m = {"table": basefilename, "sql": createsql + ";\n", "filename": filename}
        tables.append(m)

    if folder is None:
        for i in tables:
            print(i)
        for s in sqitch:
            print(s)
    else:
        for i in tables:
            print("Writing:")
            print(folder_deploy + i["table"])
            with open(folder_deploy + i["filename"], "wb") as f:
                f.write(bytes(i["sql"], 'UTF-8'))

            drop = "BEGIN;\nDROP TABLE IF EXISTS {}.{};\n".format(dbschema, i["table"])

            v_str = "select 1/count(*) from information_schema.tables where table_schema='{}' and table_name='{}';\n".format(
                dbschema, i["table"])
            verify = "BEGIN;\n" + v_str

            with open(folder_revert + i["filename"], "wb") as f:
                f.write(bytes(drop, 'UTF-8'))
                f.write(bytes("COMMIT;\n", 'UTF-8'))
            with open(folder_verify + i["filename"], "wb") as f:
                f.write(bytes(verify, 'UTF-8'))
                f.write(bytes("ROLLBACK;\n", 'UTF-8'))

        with open(folder + "sqitchplanadd_table.bash", "wb") as f:
            f.write(bytes("# This is Auto Generated from migrate_utils.py print_create_table()", 'UTF-8'))
        for s in sqitch:
            with open(folder + "sqitchplanadd_table.bash", "a") as f:
                f.write(s)

    print("Total Tables:{}".format(table_count))


def reset_migration(db):
    import sys
    db._cur.execute("""
    Drop schema if exists enforce cascade;
    Drop schema if exists stg cascade;
    Drop schema if exists sqitch cascade;
    Drop schema if exists logging cascade;
    drop schema if exists util cascade;""")
    db._cur.execute("""commit;""")
    # var=raw_input("You are about to drop 5 schemas in DATABASE:>>>{}<<< Are you Sure? Y/N ".format(db._database_name))
    # if var=="Y":
    #     db.drop_schema("stg")
    #     db.drop_schema(db.dbschema);
    #     db.drop_schema("sqitch")
    #     db.drop_schema("logging")
    #     db.drop_schema("util")
