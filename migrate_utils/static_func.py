import logging
import re
import os
import datetime


# decorator function to time a function
def timer(f):
    def wrapper(*args, **kwargs):
        start_time = datetime.datetime.now()
        logging.debug("{} :Start Time:{}".format(f.func_name, start_time))
        x = f(*args, **kwargs)
        end_time = datetime.datetime.now()
        # print(f.func_name,"End Time: ", datetime.datetime.now())
        logging.debug("{} :Ended, Duration Time: ".format(f.func_name, str(end_time - start_time)))

        return x

    return wrapper


# decorator function to time a function
def dump_params(f):
    def wrapper(*args, **kwargs):
        # print('# In function:', sys._getframe().f_code.co_name)

        print("Fucntion Name", f.__name__)
        # print(f.__code__.co_argcount)
        print(f.__code__.co_varnames[:f.func_code.co_argcount])
        fields = (f.__code__.co_varnames[:f.func_code.co_argcount])
        # print(dir(f.__code__))
        # for a in fields:
        #    print(a,kwargs.get(a,''))

        for x in kwargs:
            print(x, "kwargs")

        for arg in zip(fields, args):
            print(str(arg))
        x = f(*args, **kwargs)

        return x

    return wrapper

# invokes SED to replace every deliminter in a file to another


def sed_file_delimiter(orgfile, newfile=None, delimiter=',', new_delimiter=','):
    import subprocess
    cmd_string_double_quote = "sed -i -e 's/\"/\"\"/g' {}".format(orgfile)
    cmd_string_begin = "sed -i -e 's/^\"\"/\"/g' {}".format(orgfile)
    cmd_string_end = "sed -i -e 's/\"\"$/\"/g' {}".format(orgfile)
    cmd_string = "sed -i -e 's/\"\"{0}\"\"/\"{1}\"/g' {2}".format(delimiter, new_delimiter, orgfile)

    print("----SED---Delimiter", delimiter, new_delimiter, cmd_string_double_quote)
    subprocess.call([cmd_string_double_quote], shell=True)

    print("----SED---Delimiter", delimiter, new_delimiter, cmd_string_begin)
    subprocess.call([cmd_string_begin], shell=True)

    print("----SED---Delimiter", delimiter, new_delimiter, cmd_string_end)
    subprocess.call([cmd_string_end], shell=True)

    print("----SED---Delimiter", delimiter, new_delimiter, cmd_string)
    subprocess.call([cmd_string], shell=True)
    print("----Done SED---Delimiter")
# function that will append data to a data file
# @dump_params


def insert_each_line(orgfile, newfile, pre_pend_data, delimiter, use_header=True, has_header=True, quoted_header=False, append_file_id=True,
                     append_crc=False, db=None, table_schema=None, table_name=None, limit_rows=None,
                     header_row_location=None):
    import os
    import errno
    import hashlib
    header_added = False
    header_list_to_return = None
    return_char_unix = '\n'
    return_char_windows = '\r\n'
    start_row = 0

    if use_header and has_header:
        if header_row_location is not None:
            start_row = header_row_location + 1
        else:
            start_row = 1

    carriage_return = check_file_for_carriage_return(orgfile)

    if not os.path.exists(os.path.dirname(newfile)):
        try:
            os.makedirs(os.path.dirname(newfile))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise
    # if db connecion is provided pull the headers from db

    column_list = []

    if append_file_id:
        if quoted_header:
            column_list.append('"file_id"')
        else:
            column_list.append('file_id')
    if append_crc:
        if quoted_header:
            column_list.append('"crc"')
        else:
            column_list.append('crc')

    header_to_add = delimiter.join(column_list)

    columns_to_add_count = len(column_list)

    file_column_count = 0

    file_column_count = count_column_csv(orgfile, header_row_location=header_row_location,
                                         delimiter=delimiter) + columns_to_add_count

    # if we have no header and a db connection pull column list from db
    if use_header is False and db is not None:
        import db_utils
        assert isinstance(db, db_utils.dbconn.Connection)

        columns_from_db = db.get_columns(table_name, table_schema)
        # print(columns_from_db, "-----------xxxxxx")
        file_column_count = len(columns_from_db)

        # if column count in db is more than columns in files
        # we drop the trailing columns in the database
        # if files has more columns than db error and need to make column in database
        for i, col in enumerate(columns_from_db):
            if col not in ['file_id', 'crc']:
                column_list.append(col)

        file_column_count = count_column_csv(orgfile, header_row_location=header_row_location,
                                             delimiter=delimiter) + columns_to_add_count

        logging.info("\n\tFiles Columns Count:{}\n\tDB Column Count:{}".format(file_column_count, len(columns_from_db)))
        # stores the list of column from DB that is
        shrunk_list = []
        if len(columns_from_db) > file_column_count:
            # pulling left most column until we line up with db column_list
            # if db has less columns than file we error out anyway
            for i in range(file_column_count):
                shrunk_list.append(column_list[i])
            logging.warning("Database has more columns than File, Ignoring trailing columns:")

        if len(shrunk_list) > 0:
            column_list = shrunk_list
    # print("--------",header_to_add,column_list)

    with open(newfile, 'w') as outfile:
        # injecting a header because we are given a database connection and use_header is set to false
        # this will assure file_id and crc will always be at the front of the file
        if use_header is False and db is not None:
            column_list = delimiter.join(column_list)
            # print("-------", "writing in header")
            # print("-------", column_list + str(carriage_return))
            outfile.write(column_list + str(carriage_return))
            header_list_to_return = column_list

            header_added = True
            logging.info("\t\tFile Header:\n\t\t\t{}".format(column_list))
            if limit_rows is not None and not limit_rows.isdigit():
                logging.info("Limiting Rows was set: {}".format(limit_rows))

        with open(orgfile, 'r') as src_file:

            # making version of very similar logic so we don't have to check for append_cc on each row to do checksum
            # take care of the header first
            # if the file doesn't have a header and we have a header added it
            file_id_to_add = ''
            if append_file_id:
                file_id_to_add += '"' + pre_pend_data + '"' + delimiter

            for ii, line in enumerate(src_file):
                #print(ii, header_row_location, use_header)
                # local variable to accrue data before we write file
                if header_row_location is not None and ii == header_row_location and has_header:
                    if use_header:
                        outfile.write(header_to_add.replace('"', '') + delimiter + line.replace('"', ''))
                        header_list_to_return = str(header_to_add + delimiter + line)
                        print("usingheader", header_list_to_return)
                else:
                    data_to_prepend = file_id_to_add
                    if append_crc:
                        data_to_prepend += str(hashlib.md5(line).hexdigest()) + delimiter

                    if ii < start_row:
                        pass
                    elif limit_rows is not None and ii > limit_rows:
                        break
                    else:
                        outfile.write(data_to_prepend + line)

    count_column_csv(newfile, header_row_location=header_row_location, delimiter=delimiter)
    header_list_to_return = header_list_to_return.split(str(delimiter))

    return header_added, header_list_to_return


# playing with census stuff...WIP
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
            # cols= db.get_columns(t,schema)
            cols = t['columns']

            for col in cols:
                if p.match(col):
                    col_pivot.append(col)
                else:
                    if col in ['fileid', 'filetype', 'stusab', 'chariter', 'seq']:
                        col_base.append(col)

            sqlx = sql.format(schema, t['table_name'], ','.join(col_base),
                              ','.join("'{0}'".format(x) for x in col_base), schema + '.' + t['table_name'])
            sqlx2 = sql2.format(schema, t['table_name'], ','.join("'{0}'".format(x) for x in col_pivot), "'stats'",
                                schema + '.' + t['table_name'])

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
            # cols= db.get_columns(t,schema)
            cols = t['columns']

            for col in cols:
                if p.match(col):
                    col_pivot.append(col)
                else:
                    if col in ['fileid', 'filetype', 'stusab', 'chariter', 'seq']:
                        col_base.append(col)

            sqlx = sql.format(schema, t['table_name'], ','.join(col_base),
                              ','.join("'{0}'".format(x) for x in col_base), schema + '.' + t['table_name'])
            sqlx2 = sql2.format(schema, t['table_name'], ','.join("'{0}'".format(x) for x in col_pivot), "'stats'",
                                schema + '.' + t['table_name'])

            # print(sqlx)
            db.execute(sqlx)
            db.execute(sqlx2)


# this function will add a crc column to the table if not existing and will generate a checksum based on columns passed in
#  else it will pull list of columns from db and generate checksum for those columns and set the crc
def set_postgres_checksum_rows(db, schema, table_name, column_list=None, where_clause='1=1'):
    checksum_sql = """update {}.{} set crc=md5(row({})::text)::uuid where {}"""
    add_column(db, schema + '.' + table_name, 'crc', 'uuid', nullable='')

    if column_list is None:
        col_list = db.get_columns(table_name, schema)
    else:
        col_list = column_list
    logging.info("Running Checksum on These columns: {}".format(col_list))
    logging.info("Note order of columns matter else will result in different checksum")
    db.execute(checksum_sql.format(schema, table_name, col_list, where_clause))


# WIP
def do_postgres_upsert_insert(db, source_tbl_fqn, target_tbl_fqn):
    checksum_sql = generate_postgres_upsert(db, source_tbl_fqn, target_tbl_fqn)
    # db.execute(checksum_sql.format(schema, table_name, col_list, where_clause))


# WIP
def do_postgres_upsert_update(db, source_tbl_fqn, target_tbl_fqn):
    checksum_sql = generate_postgres_upsert(db, source_tbl_fqn, target_tbl_fqn)
    # db.execute(checksum_sql.format(schema, table_name, col_list, where_clause))


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
            # cols= db.get_columns(t,schema)
            cols = t['columns']

            for col in cols:
                if p.match(col):
                    col_pivot.append(col)
                else:
                    col_base.append(col)
            sqlx = sql.format(schema, t['table_name'], ','.join(col_base),
                              ','.join("'{0}'".format(x) for x in col_pivot), ','.join(col_pivot),
                              schema + '.' + t['table_name'])

            try:
                print("Executing:", schema, t['table_name'])
                # print(sql3.format(sqlx))
                db.execute(sql3.format(sqlx))
            except:
                print("error pivoting table:", schema, t['table_name'])
                print(sqlx)

    # base=list(set(col_base))  # pivot=list(set(col_pivot))  # print(pivot)  # print(schema,len(pivot),len(col_pivot))


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


def convert_str_snake_case(str_text):
    import inflection
    # order of these tabs matter
    tags = [" ", "(", ".", ")", "$", "-", "~", "?", "{", "}", "\\", "/", ":", "___", "__"]
    string_txt = inflection.underscore(str_text)
    for x in tags:

        string_txt = string_txt.replace(x, "_")
    string_txt = string_txt.strip()

    # print(str_text, "--------->", string_txt, "----inflection----", inflection.underscore(str_text))

    return string_txt

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
        newfield = newfield.replace(":", "_")
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


def print_padded(pad_size=10, *args):
    import sys
    # sys.stdout.write('.')
    for arg in args:
        sys.stdout.write(str(arg).ljust(pad_size))
    sys.stdout.write('\n')
    sys.stdout.flush()


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
            f.write(bytes(line))
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
                    table.append([table_schema, table_name, column_name, column_name, data_type,
                                  length])  # line=table_schema+"|"+table_name+"|"+old_column_name+"|"+column_name+"|"+data_type+"|"+length+"\n"

                f.write(bytes(make_markdown_table(table)))  # print(make_markdown_table(table))


def print_postgres_table(db, folder=None, targetschema=None):
    import os

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
            out = subprocess.check_output(
                ["pg_dump", "--schema-only", "enforce", "-t", "{}.{}".format(db.dbschema, t.name)])
        # "pg_dump -U nguyenhu enforce -t  public.temp_fl_enforcement_matters_rpt --schema-only"
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
        rows = db.query(
            "call {}.generateUpsert_style_functions('{}','{}')".format(db._database_name, db.dbschema, t.name))
        logging.debug("Generating Upsert for Table: {}".format(t.name.lower()))
        line = ("\nsqitch --plan-file functions.plan add functions/{} -n \"Adding {}\" ".format(basefilename + "_upsert", filename))

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


def print_result_json(db, query, column_header):
    import json
    result = db.query(query)
    html_header = ""
    html_data = ""
    for i, col in enumerate(column_header):
        html_header += "<th>" + col + "</th>\n"
    rows = []
    for row in result:
        x = []
        for col in row:
            x.append(str(col))
        rows.append(x)

    a = {"data": rows}

    return json.dumps(a)


def print_result_html_tr_th(db, query, column_header):
    result = db.query(query)
    html_header = ""
    html_data = ""
    for i, col in enumerate(column_header):
        html_header += "<th>" + col + "</th>\n"

    for row in result:
        tb_row = "\n<tr>"
        for col in row:
            # print(col,type(col))
            tb_row += "\t\t<td>" + str(col) + "</td>\n"
        tb_row += "</tr>\n"

        html_data += tb_row

    return html_data, html_header


def print_result_html_table(db, query, column_header, sortable_columns=None):
    result = db.query(query)
    html_header = ""
    for i, col in enumerate(column_header):
        if i in sortable_columns:
            html_header += "<th data-sort=\"string\">" + col + "</th>\n"
        else:
            html_header += "<th>" + col + "</th>\n"
    html = """<table>
    <thead>
      <tr>
        {}
      </tr>
    </thead>
    <tbody>""".format(html_header)
    for row in result:
        tb_row = "\n<tr>"
        for col in row:
            # print(col,type(col))
            tb_row += "\t\t<td>" + str(col) + "</td>\n"
        tb_row += "</tr>\n"

        html += tb_row
    html += "</tbody></table>"

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
            f.write(','.join("{0}".format(x) for x in r) + '\n')


# todo dump all types of objects to sqitch format
"""def print_create_db_obj(db, folder=None, targetschema=None, file_prefix=None,object='Tables'):
    import migrate_utils as mig
    import sqlalchemy
    import os
    sql_get_routines="SELECT routines.routine_name FROM information_schema.routines where routines.specific_schema='{}'"sql="SELECT pg_get_functiondef('{}.{}'::regproc)"
    routine_list=db.query(sql_get_routines.format(db.dbschema))
    for r in routine_list:
        code=db.query(sql.format(db.dbschema,r))

    con, meta = db.connect_sqlalchemy(db.dbschema, db._dbtype)
    # print dir(meta.tables)
    folder_deploy = folder + "/deploy/{}/".format(object)
    folder_revert = folder + "/revert/{}/".format(object)
    folder_verify = folder + "/verify/{}/".format(object)
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
    db_objects = []

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
        # print(type(n), n, t.name)
        table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
        stmt = sqlalchemy.schema.CreateTable(table)
        column_list = [c.name for c in table.columns]
        createsql = convert_sql_snake_case(str(stmt), column_list)
        logging.debug("Generating Create Statement for Table: {}".format(t.name.lower()))

        line = ("\nsqitch add tables/{}{} -n \"Adding {}\" ".format(fqn, basefilename, filename))

        sqitch.append(line)
        if targetschema is not None:
            createsql = createsql.replace(("table " + db.dbschema + ".").lower(), "table " + targetschema + ".")
        m = {"table": basefilename, "sql": createsql + ";\n", "filename": filename}
        db_objects.append(m)

    if folder is None:
        for i in db_objects:
            print(i)
        for s in sqitch:
            print(s)
    else:
        for i in db_objects:
            print("Writing:")
            print(folder_deploy + i["table"])
            with open(folder_deploy + i["filename"], "wb") as f:
                f.write(bytes(i["sql"]))

            drop = "BEGIN;\nDROP TABLE IF EXISTS {}.{};\n".format(dbschema, i["table"])
             
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
"""


def print_create_functions(db, folder=".", targetschema=None, file_prefix=None):
    import sqlalchemy
    import os
    if db._dbtype != 'POSTGRES':
        raise "Create Functions currnly only supports POSTGRES"
    sql_list = """SELECT routine_name FROM information_schema.routines 
        WHERE routine_type='FUNCTION' AND specific_schema='{}'"""

    sql_def = """SELECT pg_get_functiondef('{schema_name}.{view_name}'::regproc);"""
    error_function = []
    rs = db.query(sql_list.format(db.dbschema))
    func_list = []
    for row in rs:
        func_list.append(row[0])
    # print dir(meta.tables)
    folder_deploy = folder + "deploy/functions/"
    folder_revert = folder + "revert/functions/"
    folder_verify = folder + "verify/functions/"
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
    count = 0
    sqitch = []
    functions = []

    dbschema = targetschema or db.dbschema

    # for n, t in meta.tables.iteritems():
    for t in func_list:
        try:
            count += 1

            if file_prefix is not None:
                filename = file_prefix + t.lower() + ".sql"
                fqn = file_prefix
            else:
                filename = t.lower() + ".sql"
                fqn = ""
            basefilename = t.lower()
            rs_def = db.query(sql_def.format(schema_name=dbschema, view_name=t))

            createsql = ''
            for row in rs_def:

                createsql += row[0]

            logging.debug("Generating Create Statement for Functions: {}".format(t.lower()))

            line = ("\nsqitch --plan-file functions.plan add functions/{}{} -n \"Adding {}\" ".format(fqn, basefilename, filename))

            sqitch.append(line)

            m = {"function": basefilename, "sql": createsql + ";\n", "filename": filename}
            functions.append(m)
        except Exception as e:
            print("Error with function: {}:\n{}".format(basefilename, e))
            error_function.append(basefilename)
            db.rollback()

    if folder is None:
        for i in functions:
            print(i)
        for s in sqitch:
            print(s)
    else:
        for i in functions:
            file_path = os.path.join(folder_deploy + i["filename"])
            # print("Writing:")
            # print(file_path)
            sql_grant = """GRANT ALL ON FUNCTION {schema_name}.{function} TO operational_dba;"""
            with open(file_path, "wb") as f:
                f.write(bytes(i["sql"]))
                f.write(sql_grant.format(schema_name=dbschema, function=i["function"]))

            drop = "BEGIN;\nDROP FUNCTION IF EXISTS {schema_name}.{function};\n".format(
                schema_name=dbschema, function=i["function"])

            v_str = """SELECT 1  FROM information_schema.routines 
            WHERE routine_type='FUNCTION' AND specific_schema='{}'
            AND routine_name='{}';\n""".format(db.dbschema, i["function"])
            verify = "BEGIN;\n" + v_str
            file_path = os.path.join(folder_revert + i["filename"])
            with open(file_path, "wb") as f:
                f.write(bytes(drop))
                f.write(bytes("COMMIT;\n"))
            file_path = os.path.join(folder_verify + i["filename"])
            with open(file_path, "wb") as f:
                f.write(bytes(verify))
                f.write(bytes("ROLLBACK;\n"))

        with open(folder + "sqitchplanadd_functions.bash", "wb") as f:
            f.write(bytes("# This is Auto Generated from migrate_utils.py print_create_functions()"))
        for s in sqitch:
            with open(folder + "sqitchplanadd_functions.bash", "a") as f:
                f.write(s)

    print("Total Functions:{}".format(count))
    print("Errored Furnctions: \n{}".format(error_function))


def print_create_table(db, folder=None, targetschema=None, file_prefix=None):
    import sqlalchemy
    import os
    from sqlalchemy.dialects import postgresql

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
        # print(type(n), n, t.name)
        table = sqlalchemy.Table(t.name, meta, autoload=True, autoload_with=con)
        stmt = sqlalchemy.schema.CreateTable(table).compile(dialect=postgresql.dialect())
        column_list = [c.name for c in table.columns]
        createsql = convert_sql_snake_case(str(stmt), column_list)
        createsql = createsql.replace('"', '')

        logging.debug("Generating Create Statement for Table: {}".format(t.name.lower()))

        line = ("\nsqitch --plan-file tables.plan add tables/{}{} -n \"Adding {}\" ".format(fqn, basefilename, filename))

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
            # print("Writing:")
            #print(folder_deploy + i["table"])
            with open(folder_deploy + i["filename"], "wb") as f:
                f.write(bytes(i["sql"]))
                f.write("ALTER TABLE {}.{}\n\tOWNER TO operational_dba;".format(dbschema, i["table"]))

            drop = "BEGIN;\nDROP TABLE IF EXISTS {}.{};\n".format(dbschema, i["table"])

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


def print_create_views(db, folder=None, targetschema=None, file_prefix=None):
    import sqlalchemy
    import os
    if db._dbtype != 'POSTGRES':
        raise "Create view currnly only supports POSTGRES"
    sql_view_list = """select table_name from information_schema.tables 
                        where table_schema='{}' and table_type='VIEW'
                """
    sql_view_def = """select pg_get_viewdef('{schema_name}.{view_name}',true);"""
    rs_views = db.query(sql_view_list.format(db.dbschema))
    view_list = []
    for row in rs_views:
        view_list.append(row[0])
    # print dir(meta.tables)
    folder_deploy = folder + "/deploy/views/"
    folder_revert = folder + "/revert/views/"
    folder_verify = folder + "/verify/views/"
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

    dbschema = targetschema or db.dbschema

    # for n, t in meta.tables.iteritems():
    for t in view_list:

        table_count += 1

        if file_prefix is not None:
            filename = file_prefix + t.lower() + ".sql"
            fqn = file_prefix
        else:
            filename = t.lower() + ".sql"
            fqn = ""
        basefilename = t.lower()
        rs_view_def = db.query(sql_view_def.format(schema_name=dbschema, view_name=t))

        createsql = ''
        for row in rs_view_def:

            createsql += row[0]

        logging.debug("Generating Create Statement for VIEW: {}".format(t.lower()))

        line = ("\nsqitch --plan-file views.plan add views/{}{} -n \"Adding {}\" ".format(fqn, basefilename, filename))

        sqitch.append(line)
        sql_view = "CREATE VIEW {schema_name}.{view_name} as {sql}"
        createsql = sql_view.format(schema_name=dbschema, view_name=t, sql=createsql)
        m = {"table": basefilename, "sql": createsql + "\n", "filename": filename}
        tables.append(m)

    if folder is None:
        for i in tables:
            print(i)
        for s in sqitch:
            print(s)
    else:
        for i in tables:
            # print("Writing:")
            # print(folder_deploy + i["table"])
            sql_grant = """GRANT ALL ON TABLE {schema_name}.{table_name} TO operational_dba;"""
            with open(folder_deploy + i["filename"], "wb") as f:
                f.write(bytes(i["sql"]))
                f.write(sql_grant.format(schema_name=dbschema, table_name=i["table"]))

            drop = "BEGIN;\nDROP VIEW IF EXISTS {schema_name}.{table_name};\n".format(
                schema_name=dbschema, table_name=i["table"])

            v_str = "select 1/count(*) from information_schema.tables where table_schema='{}' and table_name='{}';\n".format(
                dbschema, i["table"])
            verify = "BEGIN;\n" + v_str

            with open(folder_revert + i["filename"], "wb") as f:
                f.write(bytes(drop))
                f.write(bytes("COMMIT;\n"))
            with open(folder_verify + i["filename"], "wb") as f:
                f.write(bytes(verify))
                f.write(bytes("ROLLBACK;\n"))

        with open(folder + "sqitchplanadd_view.bash", "wb") as f:
            f.write(bytes("# This is Auto Generated from migrate_utils.py print_create_view()"))
        for s in sqitch:
            with open(folder + "sqitchplanadd_view.bash", "a") as f:
                f.write(s)

    print("Total views:{}".format(table_count))


def reset_migration(db):
    db._cur.execute("""
    Drop schema if exists enforce cascade;
    Drop schema if exists stg cascade;
    Drop schema if exists sqitch cascade;
    Drop schema if exists logging cascade;
    drop schema if exists util cascade;""")
    db._cur.execute(
        """commit;""")  # var=raw_input("You are about to drop 5 schemas in DATABASE:>>>{}<<< Are you Sure? Y/N ".format(db._database_name))  # if var=="Y":  #     db.drop_schema("stg")  #     db.drop_schema(db.dbschema);  #     db.drop_schema("sqitch")  #     db.drop_schema("logging")  #     db.drop_schema("util")


def make_html_meta_source_files(db, full_file_path, html_head):
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

    title = " <h1>Rad Data File Log </h1>"
    formatted_html = print_result_html_table(db, sql, col_header.split(','),
                                             sortable_columns=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

    html = html_head + (str(datetime.datetime.now()) + title + formatted_html + "\n</body></html>")

    # gets all files that were attempted to be published that yield a failure
    # or no records

    if not os.path.exists(os.path.dirname(full_file_path)):
        import commands
        try:
            os.makedirs(os.path.dirname(full_file_path), 0775)

        except OSError as exc:  # Guard against race condition
            if exc.errno != exc.errno.EEXIST:
                raise
    os.chmod(os.path.dirname(full_file_path), 0776)

    with open(full_file_path, 'w') as f:
        f.write(html)
    os.chmod(full_file_path, 0776)


def make_html_publish_error(db, full_file_path, html_head):
    col_header = """
    file_id,
    file_name,
    file_path """

    sql = """SELECT distinct y.data_id as file_id,file_name,file_path
                        from (SELECT data_id  from logging.publish_log group by data_id
                        having count(*)>1 and max(row_counts)=0) x, logging.publish_log y
                        where x.data_id=y.data_id"""
    title = " <h1>Publish Error Log </h1>"
    formatted_html = print_result_html_table(db, sql, col_header.split(','), sortable_columns=[1, 2, 3, 4, 5, 6])

    html = html_head + (str(datetime.datetime.now()) + title + formatted_html + "\n</body></html>")

    # gets all files that were attempted to be published that yield a failure
    # or no records

    if not os.path.exists(os.path.dirname(full_file_path)):
        try:

            os.makedirs(os.path.dirname(full_file_path), 0775)

        except OSError as exc:  # Guard against race condition
            if exc.errno != exc.errno.EEXIST:
                raise
    os.chmod(os.path.dirname(full_file_path), 0776)
    with open(full_file_path, 'w') as f:
        f.write(html)
    os.chmod(full_file_path, 0776)


def make_html_publish_log(db, full_file_path, html_head):
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
    title = " <h1>Publish Log </h1>"
    formatted_html = print_result_html_table(db, sql, col_header.split(','), sortable_columns=[1, 2, 3, 4, 5, 6])

    html = html_head + (str(datetime.datetime.now()) + title + formatted_html + "\n</body></html>")

    # gets all files that were attempted to be published that yield a failure
    # or no records

    if not os.path.exists(os.path.dirname(full_file_path)):
        try:

            os.makedirs(os.path.dirname(full_file_path), 0775)

        except OSError as exc:  # Guard against race condition
            if exc.errno != exc.errno.EEXIST:
                raise
    os.chmod(os.path.dirname(full_file_path), 0776)
    with open(full_file_path, 'w') as f:
        f.write(html)
    os.chmod(full_file_path, 0776)


# given a columna_name,type tubple return a data word for that type
def gen_data(col):
    import random

    import sqlalchemy
    import datetime

    from random_words import RandomWords
    import operator
    assert isinstance(col, sqlalchemy.Column)

    # print(col.type,col.type.python_type)
    if str(col.type) in ['INTEGER', 'BIGINT', 'UUID', 'SMALLINT']:
        # print("----", str(col[1]),"".join([random.choice(string.digits) for i in xrange(2)]))
        data = random.randint(1, 2000)
    elif 'PRECISION' in str(col.type) or 'NUMERIC' in str(col.type):
        # print("----", str(col[1]),"".join([random.choice(string.digits) for i in xrange(2)]))
        data = random.randrange(1, 100)

    elif 'TIMESTAMP' in str(col.type):
        data = str(datetime.datetime.now())
    elif 'TEXT' in str(col.type):
        # data = "".join([random.choice(string.letters[5:26]) for i in xrange(5)])
        limit = min([100])
        if limit == 0:
            limit = 1
        word_count = random.randint(1, limit)
        rw = RandomWords()
        data = ' '.join(rw.random_words(count=word_count))
        data = data.replace('p ', r'\"')
        data = '"' + data.replace('r ', '\n') + '"'

    elif 'CHAR' in str(col.type):
        # data = "".join([random.choice(string.letters[5:26]) for i in xrange(5)])
        limit = min([5, operator.div(col.type.length, 5)])
        if limit == 0:
            limit = 1
        word_count = random.randint(1, limit)
        rw = RandomWords()
        data = ' '.join(rw.random_words(count=word_count))
    else:
        print("New DataType", str(col.type))
        data = random.randint(1, 32000)

    return str(data)


# a function that takes in a column and look at its dataype and return a function to generate random data
# that can be activated later to not have to iterate through this logic each time
def get_func(col):
    import random
    import string

    import datetime

    from random_words import RandomWords
    # assert isinstance(col, sqlalchemy.Column)

    if str(col.type) in ['INTEGER', 'BIGINT', 'SMALLINT']:
        def gen_data():
            return random.randint(1, 2000)

        return gen_data
    elif str(col.type) in ['BYTEA']:
        def gen_data():

            return "'NULL'"

        return gen_data
    elif str(col.type) in ['UUID']:

        def gen_data():
            import hashlib
            return hashlib.md5(str(random.randint(1, 2000))).hexdigest()

        return gen_data
    elif ('PRECISION' in str(col.type)
          or 'NUMERIC' in str(col.type)):
        def gen_data():
            x = random.randrange(1, 100)
            return x

        return gen_data

    elif ('TIMESTAMP' in str(col.type)):
        def gen_data():
            return str(datetime.datetime.now())

        return gen_data
    elif ('TEXT' in str(col.type)):
        # data = "".join([random.choice(string.letters[5:26]) for i in xrange(5)])

        def gen_data():
            limit = min([100])
            if limit == 0:
                limit = 1
            word_count = random.randint(1, limit)
            rw = RandomWords()
            data = ' '.join(rw.random_words(count=word_count))
            data = data.replace('p ', r'\"')
            data = '"' + data.replace('r ', '\n') + '"'
            return data

        return gen_data

    elif ('CHAR' in str(col.type)):
        # data = "".join([random.choice(string.letters[5:26]) for i in xrange(5)])
        import operator

        def gen_data():
            limit = min([3, operator.div(col.length, 5)])
            if col.length < 35:
                data = ''.join(random.choice(string.letters) for x in range(col.length))

            else:
                word_count = random.randint(1, limit)
                rw = RandomWords()
                data = ' '.join(rw.random_words(count=word_count))
                if (len(data) > col.length):
                    print("get_func: Bad Data Generated", len(data), col.length, limit, data)
            return data

        return gen_data
    else:

        def gen_data():
            print("gen-fuc", str(col.type))
            return random.randint(1, 32000)

        return gen_data

    return None


# generate data base on columns in a given table
def generate_data_sample(db, table_name, source_schema, file_name, line_count=10,
                         ignore_auto_inc_column=True, include_header=True):
    columns1 = db.get_all_columns_schema(source_schema, table_name)
    func_list = []
    column_names = []
    column_names2 = []
    for c in columns1:
        if c.autoincrement == 'NO' and c.column_name not in ['file_id', 'crc']:
            # setattr(c,'randfunc',get_func(c))
            func_list.append(get_func(c))
            column_names.append(c.column_name)

    if not os.path.exists(os.path.dirname(file_name)):
        os.makedirs(os.path.dirname(file_name), mode=0777)

    with open(os.path.abspath(file_name), 'w') as f:
        for x in range(line_count):
            line = ''
            if x == 0:
                header = ','.join([c for c in column_names])
                if include_header:
                    f.write(header + '\n')
            for i, func in enumerate(func_list):
                if (i == 0):
                    line += str(func())
                else:
                    line += "," + str(func())

            # print(x, line)
            f.write(line + '\n')


# zip up directory
# stolen from stack over flow
def zipdir(directory, target_file_name):
    import zipfile
    zf = zipfile.ZipFile(target_file_name, "w")
    for dirname, subdirs, files in os.walk(directory):
        zf.write(dirname)
        for filename in files:
            zf.write(os.path.join(dirname, filename))
    zf.close()


# Now we can iterate through all tables in db and make sample data for each table
def generate_data_sample_all_tables(db, source_schema=None, data_directory='.', line_count=10,
                                    ignore_auto_inc_column=True,
                                    zip_file_name=None, num_tables=None, post_fix='.csv',
                                    include_header=True
                                    ):
    from db_utils import dbconn

    assert isinstance(db, dbconn.Connection)
    print("Dumping: {}".format(source_schema))
    if source_schema is None:
        source_schema = db.dbschema
    # tbs = db.get_table_list(source_schema)
    tbs = db.get_table_list_via_query(source_schema)

    if not os.path.exists(os.path.dirname(data_directory)):
        os.makedirs(os.path.dirname(data_directory), mode=0777)
    print("Dumping data for scheam: {}".format(source_schema))

    for i, table_name in enumerate(tbs):
        if (num_tables is not None and i < num_tables):
            print("Generating Sample Data for Table:", table_name)
            file_name = os.path.join(data_directory, table_name + post_fix)
            generate_data_sample(db, table_name, source_schema, file_name, line_count, ignore_auto_inc_column,
                                 include_header=include_header)

    if zip_file_name is not None:
        zip_directory = os.path.dirname(zip_file_name)
        if not os.path.exists(zip_directory):
            os.makedirs(zip_directory)
        zipdir(data_directory, os.path.abspath(zip_file_name))


# this will return sql to do upsert based on the primary keys
def generate_postgres_upsert(db, table_name, source_schema, trg_schema=None, file_id=None, src_table=None):
    import db_utils.dbconn
    assert isinstance(db, db_utils.dbconn.Connection)
    if trg_schema is None:
        schema = db.dbschema
    else:
        schema = trg_schema
    if src_table is None:
        src_table = table_name

    columns = db.get_table_columns(table_name, schema)
    z = ""
    md5_src = ""
    md5_trg = ""
    first_col = -1
    for i, col in enumerate(columns):
        if i == 0:
            z += col + ' = excluded.' + col + '\n\t\t'

        else:
            z += ',' + col + ' = excluded.' + col + '\n\t\t'

        if col not in ('file_id', 'cdo_last_update'):
            first_col += 1
            if first_col == 0:
                md5_trg += 'trg.' + col + '\n\t\t'
                md5_src += 'excluded.' + col + '\n\t\t'
            else:

                md5_trg += ',trg.' + col + '\n\t\t'
                md5_src += ',excluded.' + col + '\n\t\t'

    primary_keys = db.get_primary_keys(schema + '.' + table_name)

    sql_template = """INSERT into {} as trg ({})\nSELECT {} \nFROM {}  ON CONFLICT ({}) 
    DO UPDATE SET \n\t{}\n WHERE \n\tmd5(ROW({})::Text)\n!= md5(ROW({})::Text)
                    """.format(
						        schema + '.' + table_name,
						        ',\n\t\t'.join(columns),
						        ',\n\t\t'.join(columns),
						        source_schema + '.' + src_table,
						        ','.join(primary_keys),
						        z,
						        md5_src,
						        md5_trg)

    return sql_template

# upsert syntax with no data checking


def generate_postgres_straight_upsert(db, table_name, source_schema, trg_schema=None,  src_table=None):
    import db_utils.dbconn
    assert isinstance(db, db_utils.dbconn.Connection)
    if trg_schema is None:
        schema = db.dbschema
    else:
        schema = trg_schema
    if src_table is None:
        src_table = table_name

    columns = db.get_table_columns(table_name, schema)
    z = ""
    md5_src = ""
    md5_trg = ""
    first_col = -1
    for i, col in enumerate(columns):
        if i == 0:
            z += col + ' = excluded.' + col + '\n\t\t'

        else:
            z += ',' + col + ' = excluded.' + col + '\n\t\t'

        if col not in ('file_id', 'cdo_last_update'):
            first_col += 1
            if first_col == 0:
                md5_trg += 'trg.' + col + '\n\t\t'
                md5_src += 'excluded.' + col + '\n\t\t'
            else:

                md5_trg += ',trg.' + col + '\n\t\t'
                md5_src += ',excluded.' + col + '\n\t\t'

    primary_keys = db.get_primary_keys(schema + '.' + table_name)

    sql_template = """INSERT into {} as trg ({})\nSELECT {} \nFROM {}  ON CONFLICT ({}) 
    DO UPDATE SET \n\t{}\n  
                    """.format(
        schema + '.' + table_name, ',\n\t\t'.join(columns), ',\n\t\t'.join(columns),
        source_schema + '.' + src_table,   ','.join(primary_keys),  z)

    return sql_template

# @timer

# run through the first 200 lines of a file and count the columns
# puts the counts in a list and returns the median value


def count_column_csv(full_file_path, header_row_location=0, sample_size=200, delimiter=','):
    import pandas
    import statistics

    column_count = 0
    if header_row_location is None:
        header_row_location = 0
    try:
        chunksize = 1
        chunk = None
        # print("----header row location-----", header_row_location,delimiter,full_file_path)
        count_list = []
        delim = delimiter
        for i, chunk in enumerate(
                pandas.read_table(full_file_path, chunksize=chunksize, sep=delim, header=header_row_location)):

            # just run through the file to get number of chucks

            # print(full_file_path, chunksize, delimiter, header_row_location)

            count_list.append(len(chunk.columns))
            if i > sample_size:
                break
        # print("------max------",max(count_list))
        column_count = statistics.median(count_list)
    except Exception as e:
        logging.error("Error Counting csv columns:{} \nReturning: 0".format(e))

    return int(column_count)


def check_quoted_header(full_file_path, delimiter, header_row_location=0):
    infile = open(full_file_path, 'rb')
    print("---finding quoted header", str('"' + delimiter + '"'))
    for index, line in enumerate(infile.readlines()):
        if index == header_row_location:
            if str('"' + delimiter + '"') in line:
                print("-----found quoted header-----",)
                return True
    return False

# this will read the first line of a file and determin if the file has a windows carriage return or unix


def check_file_for_carriage_return(full_file_path):
    """with open(full_file_path, 'rb') as f:
        Line_Read = f.readlines()
        if ('\r\n' in Line_Read):
            return '\r\n'
        elif ('\n' in Line_Read):
            return '\n'
        else:
            return None """
    infile = open(full_file_path, 'rb')
    for index, line in enumerate(infile.readlines()):
        if line[-2:] == '\r\n':
            return '\r\n'
        else:
            return '\n'


# @timer
def profile_csv_directory(path, delimiter=',', file_pattern=None, header_row_location=0):
    import os
    # path = './_sample_data'
    file_list = []
    total_cols_profile = {}
    if os.path.isdir(path):

        logging.info("Starting Dir:{}".format(path))
    else:
        logging.error("Invalid Directory")

    # traverse root directory, and list directories as dirs and files as files
    for root, dirs, files in os.walk(os.path.abspath(path), topdown=True):
        path = root.split(os.sep)
        print((len(path) - 1) * '---', os.path.basename(root))
        for file in files:
            print(root, file)
            file_list.append(os.path.join(root, file))
    for f in file_list:
        if file_pattern in f:
            try:
                c = None
                logging.info("Profiling file:{}".format(f))
                c = profile_csv(f, delimiter)
                for key, val in c.iteritems():
                    if total_cols_profile.get(key, 0) < val:
                        total_cols_profile[key] = val
            except Exception as e:
                logging.error("Error for file:{}".format(e))
    for key, value in sorted(total_cols_profile.items(), key=lambda x: x[1]):
        logging.info("Total:", key, value)


def profile_csv(full_file_path, delimiter=',', header_row_location=0):
    """
    Given a CSV with a header:
    The function will find the max len for each column
    :param full_file_path:
    :return: dict
    """

    def strlen(x):
        return len(str(x))

    import pandas.core.series
    count_size = 0
    chunksize = 10 ** 5

    column_profile = {}
    for i, chunk in enumerate(
            pandas.read_csv(full_file_path, header=header_row_location, engine='c', chunksize=chunksize,
                            dtype=object, index_col=False, sep=delimiter)):
        # print("process chunk:{} Delimiter:{}".format(i,delimiter))
        assert isinstance(chunk, pandas.core.frame.DataFrame)
        # print(chunk)

        for j, col in enumerate(chunk.iteritems()):
            x = col[1]
            # assert isinstance(x,pandas.core.series.Series)
            x_len = 0

            if x.dtype == 'object':
                x_len = x.map(strlen).max()

            # print(x.map(len).max(),x.name)
            y = column_profile.get(x.name, 0)
            if x_len >= y:
                column_profile[x.name] = x_len

    # for key,value in sorted(column_profile.items(), key=lambda x:x[1]):
    # print(key,value)

    # print(column_profile)
    # print(full_file_path)
    return (column_profile)


def profile_csv_testing(full_file_path, delimiter=',', header_row_location=0):
    """
    Given a CSV with a header:
    The function will find the max len for each column
    :param full_file_path:
    :return: dict
    """

    import pandas.core.series
    count_size = 0
    chunksize = 10 ** 5

    column_profile = {}
    for i, chunk in enumerate(
            pandas.read_csv(full_file_path, header=header_row_location, engine='c', chunksize=chunksize,
                            dtype=object, index_col=False, sep=delimiter)):
        # print("process chunk:{} Delimiter:{}".format(i,delimiter))
        assert isinstance(chunk, pandas.core.frame.DataFrame)
        print(chunk['Location'], chunk['Location'].map(len))
        # print(chunk.columns.values.tolist())
        for row in chunk.iterrows():
            pass
            # print(row['Location'])
            # print(type(row),row[0],row[1][0],row[1][1],row[1][2],row[1][3])

    for key, value in sorted(column_profile.items(), key=lambda x: x[1]):
        print(key, value)

    print(column_profile)
    print(full_file_path)
    return (column_profile)


def count_excel(full_file_path, sheet_number=0):
    import pandas

    logging.debug("Counting File: {}".format(datetime.datetime.now()))
    chunksize = 10 ** 5
    chunk = None
    column_count = 0
    df = pandas.read_excel(full_file_path, sheet_name=sheet_number)
    count_size = df.shape[0]
    column_count = df.shape[1]

    # print(df.columns, "^^^^ data frame columns")
    # logging.debug("Excel File Row Count:{0}".format(count_size))
    return count_size, column_count

# @timer


def count_csv(full_file_path):
    import pandas

    logging.debug("Counting File: {}".format(datetime.datetime.now()))
    chunksize = 10 ** 5
    chunk = None
    column_count = 0
    for i, chunk in enumerate(pandas.read_csv(full_file_path, chunksize=chunksize)):
        if i == 0:
            column_count = len(chunk.columns)
        # just run through the file to get number of chucks

    # count the last chunk and added to the (total chunks * i-1) to get exact total
    if i > 0:
        count_size = len(chunk) + (i - 1) * chunksize
    else:
        count_size = len(chunk)
    logging.debug("File Row Count:{0}".format(count_size))
    return count_size, column_count


def md5_file(full_file_path):
    import commands
    import platform
    md5_string = None
    os_specific_cmd = 'md5sum'

    if platform.system() == 'Linux':
        os_specific_cmd = 'md5sum'
        status_code, msg = commands.getstatusoutput("{} '{}'".format(os_specific_cmd, full_file_path))
        x = msg.split(' ')
        md5_string = x[0]
    elif platform.system() == 'Darwin':
        os_specific_cmd = 'md5'
        status_code, msg = commands.getstatusoutput("{} '{}'".format(os_specific_cmd, full_file_path))
        x = msg.split(' = ')
        md5_string = x[1]
    logging.debug("File CheckSum: {}".format(md5_string))

    return md5_string


def count_file_lines_wc(file):
    import commands
    import platform
    record_count = 0
    split_by = ' '

    if platform.system() == 'Linux':
        split_by = ' '
    elif platform.system() == 'Darwin':
        split_by = ' /'
    status_code, status_text = commands.getstatusoutput("wc -l '{}'".format(file))
    if status_code == 0:
        record_count, txt = status_text.split(split_by)
    # print(file, record_count,"----FileCount linux-------",int(record_count))
    logging.debug("FileName:{0} RowCount:{1}".format(txt, record_count))
    return int(record_count)


# adds a column to a table in datbase
def add_column(db, table_name, column_name, data_type, nullable=''):
    data_type_formatted = ''
    if data_type == "Integer":
        data_type_formatted = "INTEGER"
    elif data_type == "String":
        data_type_formatted = "VARCHAR(100)"
    elif data_type == "uuid":
        data_type_formatted = "UUID"

    base_command = ("ALTER TABLE {table_name} ADD column {column_name} {data_type} {nullable}")
    sql_command = base_command.format(table_name=table_name, column_name=column_name, data_type=data_type_formatted,
                                      nullable=nullable)
    print(sql_command)
    db.execute(sql_command)


def reset_pii(db):
    pass


def get_primary_key(db, schema, table_name):
    sql = """SELECT a.attname 
            FROM   pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                 AND a.attnum = ANY(i.indkey)
            WHERE   i.indisprimary and
            i.indrelid='{}.{}'::regclass""".format(schema, table_name)
    keys = db.query(sql)
    cols = []
    for i in keys:
        for j in i:
            cols.append(j)

    return cols


def check_pii(db):
    import datetime

    now = datetime.datetime.now()

    sql = """SELECT id,table_name,field_name,acceptable_values from compliance.health_check_rules m"""
    sql_insert = """insert into compliance.health_check_violations(health_check_rule_id,data_record_id,active,created_dt,created_by) """
    sql_on_conflict = """ ON CONFLICT (health_check_rule_id,data_record_id) DO UPDATE
                            set active=True,
                            updated_dt=now(),
                            updated_by_id='{}'
                        """.format(db._userid)

    x = db.query(sql)
    # iterate through the rules table
    for id, table_name, field_name, acceptable_values in x:
        print("Iterating through", table_name, field_name)
        # try:
        primay_key = get_primary_key(db, db.dbschema,
                                     table_name)
        # print(primay_key,type(primay_key))

        if len(primay_key) > 1:
            logging.error("More than 1 Primary Key not supported:{}".format(primay_key))

        if len(primay_key) == 0:
            logging.error("Need Primary Key:{}".format(table_name))

        key_colums_to_sql = str(primay_key[0])
        fqn_table_name = table_name
        if '.' not in table_name:
            fqn_table_name = db.dbschema + "." + table_name
        jj = acceptable_values.split(';')

        where_clause = None
        where_null = None
        in_clause = []
        for i in jj:
            if i == 'NULL':
                where_null = field_name + ' is NULL '
            else:
                in_clause.append(i)

        z_list = ','.join(("'{}'".format(x)) for x in in_clause)

        if z_list == '':
            if where_null is not None:
                where_clause = where_null
        else:
            where_clause = "lower(cast({} as varchar)) not in ({})".format(field_name, z_list)
            if where_null is not None:
                where_clause = where_clause + ' AND\n NOT ({}) '.format(where_null)

        # print(values, z_list)
        # health_checkrule_id, data_record_id, active, created_dt, created_by
        sql_none = """select 
                    '{0}' as health_checkrule_id,
                    {1} as data_record_id,  
                    True as active,
                    now() as dtm, 
                    '{2}' as created_by
                    from {3} m
                    left outer join compliance.health_check_violations h on 
                    cast(h.health_check_rule_id as integer)= {4} and h.data_record_id=cast(m.{1} as integer) and h.active=True
                    where
                    (h.id is null) AND\n (\n{5}\n) """

        sql_to_exe = sql_insert + sql_none.format(str(id), key_colums_to_sql, db._userid, fqn_table_name, id,   where_clause)
        print(sql_to_exe)
        db.execute(sql_to_exe)
        # except Exception as e:
        # logging.error("Error processing table:{} \n{}".format(table_name,e))


# stole from stack overflow
# https://stackoverflow.com/questions/305378/list-of-tables-db-schema-dump-etc-using-the-python-sqlite3-api
def sqlite_to_csv(full_file_path, out_file_path=None):
    import sqlite3
    import pandas as pd
    db = sqlite3.connect(full_file_path)
    abs_file_path = os.path.dirname(".")
    if out_file_path is not None:
        abs_file_path = os.path.dirname(out_file_path)

    cursor = db.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    for table_name in tables:
        table_name = table_name[0]
        table = pd.read_sql_query("SELECT * from %s" % table_name, db)
        print("Extracting table: {}".format(table_name))
        table.to_csv(os.path.join(abs_file_path, (table_name + '.csv')),
                     index_label='index', header=True, index=False, encoding='utf-8')


def sql_to_excel(db, sql_string, full_file_path, column_names):
    import xlsxwriter
    workbook = xlsxwriter.Workbook(full_file_path)
    worksheet = workbook.add_worksheet()
    row = 0
    col = 0
    rs = db.query(sql_string)
    header = column_names.split(',')
    # Write the header
    print('Writing Excel File')
    print('file name', full_file_path)
    print('Columns', column_names)
    for columna_name in header:
        worksheet.write(row, col, columna_name)
        col += 1
    # for each row
    for r in rs:
        row += 1
        col = 0
        # for each column
        for c in r:
            worksheet.write(row, col, r[col])
            col += 1

    print('Records written', row)
