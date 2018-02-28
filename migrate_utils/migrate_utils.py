import logging
import re
import os
import datetime

def dd_lookup_uuid(db, schema, table_name_regex, col_regex, cols_to_retain=None, keep_nulls=False):

    tables = db.get_tables(schema=schema)
    create_string = """create table if not exists _tmp_test_{} as select * from {} limit 1"""
    t_compiled = re.compile(table_name_regex)
    sql = """
    insert into census.dd_lookup_uuid( year,table_name,lookup_code,lookup_type)
     select distinct '{}','{}' , unnest(array[{}]) as col_name, unnest(array[{}])   from {} 
     on conflict do nothing
    """
    sql2 = """
    insert into census.dd_lookup_uuid(year,table_name,lookup_code,lookup_type)
     select distinct '{}','{}' , unnest(array[{}]) as col_name,  {} 
     -- from {} 
     on conflict do nothing
    """
    p = re.compile(col_regex)

    for t in tables:
        # print(type(t),t)
        col_base = []
        col_pivot = []
        if t_compiled.match(t['table_name']):
            # print("----",t)
            #cols= db.get_columns(t,schema)
            cols = t['columns']

            for col in cols:
                if p.match(col):
                    col_pivot.append(col)
                else:
                    if col in ['fileid', 'filetype', 'stusab', 'chariter', 'seq']:
                        col_base.append(col)

            sqlx = sql.format(schema,
                              t['table_name'], ','.join(col_base), ','.join("'{0}'".format(x) for x in col_base), schema + '.' + t['table_name'])
            sqlx2 = sql2.format(schema,
                                t['table_name'], ','.join("'{0}'".format(x) for x in col_pivot), "'stats'", schema + '.' + t['table_name'])

            # print(sqlx)
            db.execute(sqlx)
            db.execute(sqlx2)


def dd_lookup(db, schema, table_name_regex, col_regex, cols_to_retain=None, keep_nulls=False):

    tables = db.get_tables(schema=schema)
    create_string = """create table if not exists _tmp_test_{} as select * from {} limit 1"""
    t_compiled = re.compile(table_name_regex)
    sql = """
    insert into census.dd_lookup(year,table_name,lookup_code,lookup_type)
     select distinct '{}','{}' , unnest(array[{}]) as col_name, unnest(array[{}])   from {} 
     on conflict do nothing
    """
    sql2 = """
    insert into census.dd_lookup(year,table_name,lookup_code,lookup_type)
     select distinct '{}','{}' , unnest(array[{}]) as col_name,  {} 
     -- from {} 
     on conflict do nothing
    """
    p = re.compile(col_regex)

    for t in tables:
        # print(type(t),t)
        col_base = []
        col_pivot = []
        if t_compiled.match(t['table_name']):
            # print("----",t)
            #cols= db.get_columns(t,schema)
            cols = t['columns']

            for col in cols:
                if p.match(col):
                    col_pivot.append(col)
                else:
                    if col in ['fileid', 'filetype', 'stusab', 'chariter', 'seq']:
                        col_base.append(col)

            sqlx = sql.format(schema,
                              t['table_name'], ','.join(col_base), ','.join("'{0}'".format(x) for x in col_base), schema + '.' + t['table_name'])
            sqlx2 = sql2.format(schema,
                                t['table_name'], ','.join("'{0}'".format(x) for x in col_pivot), "'stats'", schema + '.' + t['table_name'])

            # print(sqlx)
            db.execute(sqlx)
            db.execute(sqlx2)


def pivot_table(db, schema, table_name_regex, col_regex, cols_to_retain=None, keep_nulls=False):

    tables = db.get_tables(schema=schema)
    create_string = """create table if not exists _tmp_test_{} as select * from {} limit 1"""
    t_compiled = re.compile(table_name_regex)
    sql = """
    
     SELECT cast('{}' as varchar) as year,
     cast('{}' as varchar) as table_name,{} , unnest(array[{}]) as col_name, unnest(array[{}]) as col_val from {} 
    """
    sql3 = """ INSERT into census.seq_data2(dd_chariter,dd_filetype,dd_stusab,dd_seq,dd_stat,stat_val,geoid,logrecno,dd_fileid)
    SELECT aa.id,bb.id,cc.id,dd.id,ee.id,col_val,geoid,logrecno,ff.id FROM ({}
    ) AS x
    join census.dd_lookup aa on aa.table_name=x.table_name and aa.year=x.year  and aa.lookup_code=x.chariter and aa.lookup_type='chariter' 
    join census.dd_lookup bb on bb.table_name=x.table_name and bb.year=x.year  and bb.lookup_code=x.filetype and bb.lookup_type='filetype' 
    join census.dd_lookup cc on cc.table_name=x.table_name and cc.year=x.year  and cc.lookup_code=x.stusab and cc.lookup_type='stusab' 
    join census.dd_lookup dd on dd.table_name=x.table_name and dd.year=x.year  and dd.lookup_code=x.seq and dd.lookup_type='seq'
    join census.dd_lookup ee on ee.table_name=x.table_name and ee.year=x.year  and ee.lookup_code=x.col_name and ee.lookup_type='stats'
    join census.dd_lookup ff on ff.table_name=x.table_name and ff.year=x.year  and ff.lookup_code=x.fileid and ff.lookup_type='fileid'
    where x.col_val is not null 
    on conflict do nothing ;
    """
    p = re.compile(col_regex)

    for t in tables:
        # print(type(t),t)
        col_base = []
        col_pivot = []
        if t_compiled.match(t['table_name']):
            # print("----",t)
            #cols= db.get_columns(t,schema)
            cols = t['columns']

            for col in cols:
                if p.match(col):
                    col_pivot.append(col)
                else:
                    col_base.append(col)
            sqlx = sql.format(
                schema, t['table_name'], ','.join(col_base), ','.join("'{0}'".format(x) for x in col_pivot), ','.join(col_pivot), schema + '.' + t['table_name'])

            try:
                print("Executing:", schema, t['table_name'])
                # print(sql3.format(sqlx))
                db.execute(sql3.format(sqlx))
            except:
                print("error pivoting table:", schema, t['table_name'])
                print(sqlx)

    # base=list(set(col_base))
    # pivot=list(set(col_pivot))
    # print(pivot)
    # print(schema,len(pivot),len(col_pivot))


def change_table_owner(db, schema, owner_name):
    query = """SELECT 'ALTER TABLE '|| schemaname || '.' || tablename ||' OWNER TO operational_dba;'
    FROM pg_tables WHERE   schemaname IN ('{}')
    ORDER BY schemaname, tablename;

    """.format(schema)

    resultset = db.query(query)
    db.execute('GRANT ALL ON SCHEMA {} TO operational_dba;'.format(schema))
    for r in resultset:
        db.execute(r[0])


def change_view_owner(db, schema, owner_name):
    query = """SELECT 'ALTER VIEW '|| table_schema || '.' || table_name ||' OWNER TO operational_dba;'
FROM information_schema.views WHERE  table_schema  IN ('{}')
ORDER BY table_schema, table_name;
    """.format(schema)

    resultset = db.query(query)
    for r in resultset:
        db.execute(r[0])


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

    newlist = []
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

    sql = """select usename
        -- ,rolname 
        from pg_user
        join pg_auth_members on (pg_user.usesysid=pg_auth_members.member)
        join pg_roles on (pg_roles.oid=pg_auth_members.roleid)
        where rolname='{}_readonly'
        or rolname='{}_readonly
        ;""".format(db._database_name, db.dbschema)
    return db.query(sql)


def appdend_to_readme(db, folder=None, targetschema=None):
    with open("../README.md") as f:
        content = f.readlines()
    dictionary = "<a name=\"data_dictionary\"></a>"[:25]
    dict_query = """select table_schema,table_name,column_name,data_type,character_maximum_length
                from information_schema.columns a 
                where table_schema='enforce' order by table_name,ordinal_position"""
    header = ["table_schema" + "|" + "table_name" + "|" + "column_name" + "|" + "data_type" + "|", "length"]
    header = ["table_schema", "table_name", "old_column_name", "column_name", "data_type", "length"]
    pad_size = 30
    table = [header]
    with open(folder + "/README.md", "wb") as f:
        for line in content:
            f.write(bytes(line, 'UTF-8'))
            if line[:25] == dictionary[:25]:
                # f.write(bytes(header,'UTF-8'))
                rows = db.query(dict_query)
                for row in rows:
                    table_schema = ""
                    table_name = ""
                    column_name = ""
                    data_type = ""
                    length = ""

                    table_schema = row[0]
                    table_name = row[1].rjust(pad_size, " ")
                    column_name = row[2].rjust(pad_size, " ")
                    data_type = row[3].rjust(pad_size, " ")
                    if row[4] is not None:
                        length = str(row[4])
                    length = length.rjust(6, " ")
                    # table.append([table_schema,table_name,column_name,data_type,length])
                    # table.append(row)
                    table.append([table_schema, table_name, column_name, column_name, data_type, length])
                    # line=table_schema+"|"+table_name+"|"+old_column_name+"|"+column_name+"|"+data_type+"|"+length+"\n"

                f.write(bytes(make_markdown_table(table), 'UTF-8'))
        # print(make_markdown_table(table))


def print_postgres_table(db, folder=None, targetschema=None):
    import migrate_utils as mig
    import sqlalchemy
    import os
    from sqlalchemy.dialects import postgresql
    import subprocess

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_table = folder + "/postgrestables/"

    os.makedirs(folder_table)

    sqitch = []
    table_count = 0
    # for n, t in meta.tables.iteritems():
    for n, t in meta.tables.items():
        table_count += 1
        filename = t.name.lower() + ".sql"
        basefilename = t.name.lower()
        out = None
        try:
            out = subprocess.check_output(["pg_dump", "--schema-only", "enforce", "-t", "{}.{}".format(db.dbschema, t.name)])
    #"pg_dump -U nguyenhu enforce -t  public.temp_fl_enforcement_matters_rpt --schema-only"
            # print(out)
        except subprocess.CalledProcessError as e:
            print(e)

        logging.debug("Generating Postgres Syntax Table: {}".format(t.name.lower()))

        with open(folder_table + filename, "wb") as f:
            f.write(out)
            f.write(bytes("\n"))

    print("Total Tables:{}".format(table_count))


def print_create_table_upsert(db, folder=None, targetschema=None):
    import os

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_deploy = folder + "/deploy/functions/"
    folder_revert = folder + "/revert/functions/"
    folder_verify = folder + "/verify/functions/"
    try:
        os.makedirs(folder_deploy)
    except:
        pass
    try:
        os.makedirs(folder_revert)
    except:
        pass
    try:
        os.makedirs(folder_verify)
    except:
        pass

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
                f.write(bytes(line[0]))
                f.write(bytes("\n"))

        drop = "DROP FUNCTION IF EXISTS {}.{};".format(db.dbschema, basefilename + "_upsert();")
        with open(folder_revert + filename, "wb") as f:
            f.write(bytes(drop))
            f.write(bytes("\n"))

        with open(folder_verify + filename, "wb") as f:
            f.write(bytes("-- NA "))
            f.write(bytes("\n"))
    print("Total Tables:{}".format(table_count))
    with open(folder + "/sqitchplanadd_upsert.bash", "wb") as f:
        f.write(bytes("# This is Auto Generated from migrate_utils.py print_create_table_upsert()"))
    for s in sqitch:
        with open(folder + "/sqitchplanadd_upsert.bash", "a") as f:
            f.write(s)

# prints csv file in artifacts directory for each table in a dbschema
def print_result_html_table(db,query,column_header,sortable_columns=None):
    result=db.query(query)
    html_header=""
    for i,col in enumerate(column_header):
        if i in sortable_columns:
            html_header+="<th data-sort=\"string\">"+col+"</th>\n"
        else:
            html_header+="<th>"+col+"</th>\n"
    html="""<table>
    <thead>
      <tr>
        {}
      </tr>
    </thead>
    <tbody>""".format(html_header)
    for row in result: 
        tb_row="\n<tr>"
        for col in row:
            #print(col,type(col))
            tb_row+="\t\t<td>"+str(col)+"</td>\n"
        tb_row+="</tr>\n"

        html+=tb_row
    html+="</tbody></table>"

    return html

# prints csv file in artifacts directory for each table in a dbschema
def print_table_dict(db, folder='.', targetschema=None):

    if targetschema is None:
        dbschema = db.dbschema
    else:
        dbschema = targetschema

    postgres_sql = """SELECT distinct case when v.table_name is null then 'Table'
        else 'View'
        end  as x,a.table_name,a.column_name,a.data_type,character_maximum_length as length ,is_nullable,a.ordinal_position
    from information_schema.columns a
    left outer join  information_schema.views v on a.table_schema=v.table_schema and a.table_name=v.table_name
    where a.table_schema='{}'   
    order by 1,2, a.ordinal_position """.format(dbschema)

    rs = db.query(postgres_sql)

    # print dir(meta.tables)
    folder_dict = folder + "/artifacts"
    try:
        print("making folder:", folder_dict)
        os.makedirs(folder_dict)
    except Exception as e:
        print("failed making folder:", folder_dict, e)
    with open(folder_dict + '/table_dictionary.csv', "wb") as f:
        f.write('TYPE,TABLE_NAME,COLUMN_NAME,DATA_TYPE,LENGTH ,IS_NULLABLE,ORDINAL_POSITION' '\n')
        for r in rs: 
            f.write(','.join("{0}".format(x) for x in r)+ '\n')


def print_create_table(db, folder=None, targetschema=None, file_prefix=None):
    import migrate_utils as mig
    import sqlalchemy
    import os

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_deploy = folder + "/deploy/tables/"
    folder_revert = folder + "/revert/tables/"
    folder_verify = folder + "/verify/tables/"
    try:
        os.makedirs(folder_deploy)
    except:
        pass
    try:
        os.makedirs(folder_revert)
    except:
        pass
    try:
        os.makedirs(folder_verify)
    except:
        pass
    table_count = 0
    sqitch = []
    tables = []

    if targetschema is None:
        dbschema = db.dbschema

    else:
        dbschema = targetschema

    # for n, t in meta.tables.iteritems():
    for n, t in meta.tables.items():
        table_count += 1

        if file_prefix is not None:
            filename = file_prefix + t.name.lower() + ".sql"
            fqn = file_prefix
        else:
            filename = t.name.lower() + ".sql"
            fqn = ""
        basefilename = t.name.lower()
        #print(type(n), n, t.name)
        table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
        stmt = sqlalchemy.schema.CreateTable(table)
        column_list = [c.name for c in table.columns]
        createsql = mig.convert_sql_snake_case(str(stmt), column_list)
        logging.debug("Generating Create Statement for Table: {}".format(t.name.lower()))

        line = ("\nsqitch add tables/{}{} -n \"Adding {}\" ".format(fqn, basefilename, filename))

        sqitch.append(line)
        if targetschema is not None:
            createsql = createsql.replace(("table " + db.dbschema + ".").lower(), "table " + targetschema + ".")
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
                f.write(bytes(i["sql"]))

            drop = "BEGIN;\nDROP TABLE IF EXISTS {}.{};\n".format(dbschema, i["table"])
            print(dbschema, "-----db---")
            v_str = "select 1/count(*) from information_schema.tables where table_schema='{}' and table_name='{}';\n".format(
                dbschema, i["table"])
            verify = "BEGIN;\n" + v_str

            with open(folder_revert + i["filename"], "wb") as f:
                f.write(bytes(drop))
                f.write(bytes("COMMIT;\n"))
            with open(folder_verify + i["filename"], "wb") as f:
                f.write(bytes(verify))
                f.write(bytes("ROLLBACK;\n"))

        with open(folder + "sqitchplanadd_table.bash", "wb") as f:
            f.write(bytes("# This is Auto Generated from migrate_utils.py print_create_table()"))
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



def make_html_meta_source_files(db,full_file_path,html_head):


    col_header = """file_id,
      file_name,
      file_path,  
      file_type,
      file_process_state,
      database_table,
      process_start_dtm,
      process_end_dtm,
      current_worker_host, 
      rows_inserted,
      file_size,
      total_rows, 
      total_files_processed,
      last_error_msg """

    sql = """SELECT id,file_name,
      replace(file_path,'/home/dtwork/dw/file_transfers',''), 
      file_type,
      file_process_state,
      database_table,
      process_start_dtm,
      process_end_dtm,
      current_worker_host, 
      rows_inserted, 
      file_size,
      total_rows, 
      total_files_processed,
      last_error_msg  from logging.meta_source_files"""


    title=" <h1>Rad Data File Log </h1>"
    formatted_html = print_result_html_table(db, sql, col_header.split(','),sortable_columns=[1,2,3,4,5,6,7,8,9,10])

    html=html_head + (str(datetime.datetime.now())+title+
                formatted_html +
                "\n</body></html>")


    # gets all files that were attempted to be published that yield a failure or no records

    with open(full_file_path, 'w') as f:
        f.write(html)
    os.chmod(full_file_path, 0o666)


def make_html_publish_error(db,full_file_path,html_head):

    col_header = """
    file_id,
    file_name,
    file_path """

    sql = """SELECT distinct y.data_id as file_id,file_name,file_path
                        from (SELECT data_id  from logging.publish_log group by data_id
                        having count(*)>1 and max(row_counts)=0) x, logging.publish_log y
                        where x.data_id=y.data_id"""
    title=" <h1>Publish Error Log </h1>"
    formatted_html = print_result_html_table(db, sql, col_header.split(','),sortable_columns=[1,2,3,4,5,6])

    html=html_head + (str(datetime.datetime.now())+title+
                formatted_html +
                "\n</body></html>")


    # gets all files that were attempted to be published that yield a failure or no records

    with open(full_file_path, 'w') as f:
        f.write(html)
    os.chmod(full_file_path, 0o666)


def make_html_publish_log(db,full_file_path,html_head):

    col_header = """
        data_id  ,
      publish_start_time  ,
      publish_end_time  ,
      table_name   , 
      row_counts  ,
      file_name  ,
      file_path,
       message     """

    sql = """SELECT data_id  ,
      publish_start_time  ,
      publish_end_time  ,
      table_name   , 
      row_counts  ,
      file_name  ,
      file_path,
       message  from logging.publish_log where row_counts>0"""
    title=" <h1>Publish Log </h1>"
    formatted_html = print_result_html_table(db, sql, col_header.split(','),sortable_columns=[1,2,3,4,5,6])

    html=html_head + (str(datetime.datetime.now())+title+
                formatted_html +
                "\n</body></html>")


    # gets all files that were attempted to be published that yield a failure or no records

    with open(full_file_path, 'w') as f:
        f.write(html)
    os.chmod(full_file_path, 0o666)
