import yaml
import multiprocessing as mp
import argparse
import os
import shutil
from py_dbutils import parents as db_utils
import socket
import sys
import pprint
import logging as lg
 
import datetime
import copy
 

lg.basicConfig()
logging = lg.getLogger()
# logging.setLevel(lg.INFO)
logging.setLevel(lg.DEBUG)
# target_table:
# target_db_type:
# source_db_type:
# delivery_type:
# source_sql:
# source_table:
 
WORKINGPATH=os.environ.get('WORKINGPATH',None)
 
DELIVERY_TRUNCATE = 'KILLFILL'
DELIVERY_FILE_ID = 'FILE_ID'
DELIVERY_LAST_UPDATE = 'CDO_LAST_UPDATE'
DELIVERY_PROJECT = 'PROJECT'
DELIVERY_SCHEMA = 'SCHEMA'
CURR_PID = os.getpid()
CURR_HOST = socket.gethostname()
LOGGING_TBL_SQL = """CREATE TABLE {0}
(
    id serial,
    trg_table character varying COLLATE pg_catalog."default" ,
    src_sql character varying COLLATE pg_catalog."default",
    copy_status character varying COLLATE pg_catalog."default",
    load_status  character varying COLLATE pg_catalog."default",
    start_time timestamp without time zone,
    end_time timestamp without time zone,
     PRIMARY KEY (id)
) """
WORK_TBL_SQL = """CREATE TABLE {0}
(
    src_sql_hash uuid,
    src_sql character varying COLLATE pg_catalog."default",
    trg_table character varying COLLATE pg_catalog."default" ,
    pid integer,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    state character varying,
     PRIMARY KEY (src_sql_hash)
) """

# runs through a dict str field and replace the values in {{key_name}} to string value of the one in the base_dict


def recurse_replace_yaml(p_trg_data, p_base_dict):
    def inject_yaml_data(str_data, yaml):

        dict_keys = yaml.keys()
        # print('before ', str_data)
        for key in dict_keys:
            if isinstance(yaml[key], dict):
                str_data=recurse_replace_yaml(str_data,yaml[key])
            elif not (isinstance(yaml[key], list) or isinstance(yaml[key], dict)):
                str_data = str_data.replace("{{" + key + "}}", str(yaml[key]))
        # print('after ', str_data)
        return str_data

    assert isinstance(p_base_dict, dict), "2nd parameter has to be TYPE dict: {}".format(type(p_base_dict))
    if isinstance(p_trg_data, list):
        new_list = []
        for trg_item in p_trg_data:
            if isinstance(trg_item, str):
                trg_item = inject_yaml_data(trg_item, p_base_dict)
            elif isinstance(trg_item, list) or isinstance(trg_item, dict):
                if isinstance(trg_item, dict):
                    trg_item = recurse_replace_yaml(trg_item, trg_item)
                trg_item = recurse_replace_yaml(trg_item, p_base_dict)
            new_list.append(trg_item)
        p_trg_data = new_list
        # pprint.pprint(p_trg_data)
    elif isinstance(p_trg_data, dict):
        # v_trg_dict = copy.deepcopy(p_trg_dict)
        # pprint.pprint(p_trg_data)
        new_dict = {}
        for trg_key in p_trg_data.keys():
            trg_item = p_trg_data[trg_key]
            if isinstance(trg_item, str):
                if trg_key=='logging_table':
                    print(trg_item)
                    import pprint 
                    pprint.pprint(p_base_dict)
                trg_item = inject_yaml_data(trg_item, p_base_dict)
            elif isinstance(trg_item, list) or isinstance(trg_item, dict):
                trg_item = recurse_replace_yaml(trg_item, p_base_dict)
            new_dict[trg_key] = trg_item
        p_trg_data = new_dict
    elif isinstance(p_trg_data, str):
        p_trg_data = inject_yaml_data(p_trg_data, p_base_dict)

    return p_trg_data


def log_move_data(trg_db, logging_table, trg_table_name, sql_string, copy_status, load_status, start_time, end_time):
    v_insert_sql = """insert into {logging_table}(trg_table, src_sql, copy_status, load_status, start_time, end_time) values({data_val})"""
    v_update_sql = """update {table_name} set end_time=now() where trg_table='{trg_table}'"""

    if not trg_db.table_exists(logging_table):
        trg_db.execute(LOGGING_TBL_SQL.format(logging_table))
        trg_db.execute('commit;')
        trg_db.execute(
            'ALTER TABLE {0} OWNER to operational_dba;'.format(logging_table))
    data_val = """'{0}','{1}','{2}','{3}','{4}','{5}'""".format(
        trg_table_name, sql_string, copy_status, load_status, start_time, end_time)
    trg_db.execute(v_insert_sql.format(
        logging_table=logging_table, data_val=data_val))


def move_data(sql_string, trg_table_name, src_db, trg_db, label='', skip_if_exists=False, retain_data_file_path=None, logging_table=None, pk=None, work_table=None):
    # stupid util that take sql and dumps to file and loads the file into target table
    # no column header, assumes table structure matches the sql data
    if not os.path.exists(os.path.dirname(WORKINGPATH)):
        try:
            os.makedirs(os.path.dirname(WORKINGPATH))
        except OSError as exc:  # Guard against race condition
            
            logging.error("Error in move data making WORKINGPATH: {}".format(str(exc)))
            raise

    if retain_data_file_path is not None and not os.path.exists(os.path.dirname(retain_data_file_path)):
        try:

            os.makedirs(os.path.dirname(retain_data_file_path))
        except OSError as exc:  # Guard against race condition
            
            logging.error("Error in movde data making retain_data_file_path: {}".format(str(exc)))
            raise
    tmp_file_name = os.path.join(
        WORKINGPATH, '_tmp_{0}{1}.csv'.format(trg_table_name, label))
    copy_status = None
    v_start_time = datetime.datetime.now()
    if not (os.path.exists(os.path.dirname(tmp_file_name)) and skip_if_exists):
        
        copy_status = src_db.copy_to_csv(sql_string, tmp_file_name, ',')
        set_state(trg_db, work_table, pk,
                  'Rows Dumped: {}'.format(copy_status))
    else:
        logging.warning(
            "Previous copy of file found and skip_if_exists is True")
    logging.info("PID: {} Dummping Data:".format(os.getpid()))
    load_status = 0
    if int(copy_status) > 0:
        # load_status = trg_db.import_file_client_side(
        #     tmp_file_name, trg_table_name, ',')
            
        load_status = trg_db.import_pyscopg2_copy(
            tmp_file_name, trg_table_name, ',')
    set_state(trg_db, work_table, pk, 'Rows Inserted: {}'.format(load_status))

    if logging_table is not None:
        v_end_time = datetime.datetime.now()
        logging.info("PID: {} Inserting {} records:".format(
            os.getpid(), load_status))
        log_move_data(trg_db, logging_table, trg_table_name, sql_string,
                      copy_status, load_status, v_start_time, v_end_time)

    if int(copy_status) != int(load_status):
        set_state(trg_db, work_table, pk, 'Exception: copy_status != load_status: {}:{}'.format(
            copy_status, load_status))
        raise Exception(
            "Rows Loaded Do not match rows Dumped! Please Check this file:\n\t\t{0}".format(tmp_file_name))

    else:
        if retain_data_file_path is not None:

            backupfile = os.path.join(
                retain_data_file_path, '{0}{1}.csv'.format(trg_table_name, label))
            logging.info("Retaining File: {}: \n\t{}".format(
                tmp_file_name, backupfile))
            # os.rename(tmp_file_name, backupfile)
            shutil.move(tmp_file_name, backupfile)
        else:
            os.remove(tmp_file_name)
    return load_status


def set_state(trg_db, work_table, pk, comment):
    sql = """update {} set state=concat('{}\n',state) where src_sql_hash='{}'""".format(
        work_table, comment, pk)
    trg_db.execute(sql)


def move_item(yaml, src_db, trg_db, p_trg_table, p_source_sql, src_yaml=None, Truncate=False, pk=None, work_table=None):
    # step 1: Truncate Table in target database connection
    # step 2 & 3: Dump the Data to CSV standard

    target_table_name = p_trg_table
    source_sql = p_source_sql
    logging_table = recurse_replace_yaml(yaml.get('logging_table', yaml), src_yaml)
    retain_data_file_path = yaml.get('retain_data_file_path', None)
    v_move_data = yaml.get('move_data', False)
    if v_move_data:
        if Truncate:
            trg_db.execute(
                'TRUNCATE TABLE {} CASCADE'.format(target_table_name))
        load_status = move_data(source_sql, target_table_name, src_db, trg_db, label=os.getpid(),
                                retain_data_file_path=retain_data_file_path, logging_table=logging_table, pk=pk, work_table=work_table)

        trg_db.execute('COMMIT')
        # trg_db.execute('VACUUM analyze {}'.format(target_table_name))
    else:
        logging.debug("Not Moving Data Set 'move_data' attribute to True in Yaml files")
        return 0
    return load_status


def plan_work(publish_item, src_db, trg_db):
    pprint.pprint(publish_item)
    truncate_target = publish_item['db']['target_db'].get('truncate_target', False)
    source_schema = publish_item['db']['source_db']['schema']
    target_schema = publish_item['db']['target_db']['schema']

    partition_column = publish_item.get('partition_column', None)
    v_trg_table = None
    v_src_table = None
    tables = []
    all_yaml_tables = []
    source_tables = publish_item.get('tables', [])
    all_tables = publish_item.get('all_tables', False)
    drop_recreate = publish_item['db']['target_db'].get('drop_recreate', False)

    create_target_tables = publish_item['db']['target_db'].get('create_target_tables', False)

    for table in source_tables:
        if table.get('table', None) is not None:
            all_yaml_tables.append(table['table'])
    if len(source_tables) == 0 or all_tables:
        # print(all_yaml_tables)
        for t in src_db.get_table_list_via_query(source_schema):
            if t not in all_yaml_tables:
                tables.append(t)
    else:
        tables = source_tables
    # print(tables)
    # print(source_tables)

    def get_migration_list(source_tables):
         
        for src_table in source_tables:
            #table = recurse_replace_yaml(src_table, src_table)
            #table = recurse_replace_yaml(table, publish_item)
            table = src_table
            all_tables_batch = False
            migration_list = []
            v_trg_table = None
            v_src_table = None
            logging.info('Migrating Table: {}'.format(table))
            migration_item = dict(publish_item)


    
            if isinstance(table, str):
                migration_item['migration_params'] = dict(
                    {'batch_method': 'ALL'})
            else:
                 
                #pprint.pprint(migration_item)
                migration_item['migration_params'] = dict(table)
                assert isinstance(table, dict)
                all_tables_batch = table.get('all_tables', False)

            if isinstance(table, dict):
                if all_tables_batch == False:
                    v_trg_table = target_schema + '.' + table['table']
                    v_src_table = source_schema + '.' + table['table']

                if table['batch_method'] == 'ALL':
                    migration_item['source_sql'] = 'select * from {0}'.format(
                        v_src_table)
                    migration_item['target_table'] = v_trg_table
                    migration_item['source_table'] = v_src_table
                    migration_list.append(migration_item)
                elif table['batch_method'] == 'COLUMN':

                    v_batch_method_column = table['column']

                    migration_item['column'] = v_batch_method_column
                    if all_tables_batch:

                        mig_item = None
                        mig_item = copy.deepcopy(migration_item)
                        source_tables = src_db.get_table_list_via_query(
                            source_schema)
                        for src_tbl in source_tables:
                            tbl = recurse_replace_yaml(src_tbl, src_tbl)
                            already = False
                            for t in all_yaml_tables:
                                if tbl == t:
                                    already = True
                            # do not add table if we have it in yaml file
                            if already == False:
                                fqn_src_tbl = str(source_schema + '.' + tbl)
                                fqn_trg_tbl = str(target_schema + '.' + tbl)
                                mig_item['chunk_sql'] = table.get('chunk_sql', None) or 'select distinct {} from {}'.format(
                                    mig_item['column'], fqn_src_tbl)
                                mig_item['source_sql'] = 'select * from {0}'.format(
                                    fqn_src_tbl)
                                mig_item['target_table'] = fqn_trg_tbl + ''
                                mig_item['source_table'] = fqn_src_tbl + ''

                                # time.sleep(10)
                                migration_list.append(copy.deepcopy(mig_item))

                    else:
                        migration_item['chunk_sql'] = table['chunk_sql']
                        migration_item['source_sql'] = 'select * from {0}'.format(
                            v_src_table)
                        migration_item['target_table'] = v_trg_table
                        migration_item['source_table'] = v_src_table
                        migration_list.append(migration_item)
            else:
                v_trg_table = target_schema + '.' + table
                v_src_table = source_schema + '.' + table

                migration_item['source_sql'] = 'select * from {0}'.format(
                    v_src_table)

                migration_item['target_table'] = v_trg_table
                migration_item['source_table'] = v_src_table
                migration_list.append(migration_item)

            # print(migration_list)

            add_work(trg_db, src_db, migration_list)

            if drop_recreate:
                trg_db.execute('DROP TABLE {0} cascade;'.format(v_trg_table))
                trg_db.commit()
            if create_target_tables:
                create_data_table(trg_db, src_db, v_src_table,
                                  v_trg_table, partition_column)

    if source_tables is not None:
        #print("runt source_tables")
        get_migration_list(source_tables)
    if tables is not None:
        #print("run tables")
        get_migration_list(tables)
    if tables is None and source_tables is None:
        raise ValueError("No tables found to Create Plan")

    # x=(src_db.get_create_table_cli('bk_mpo.loan_month','bk.loan_month'))
    # move_project(publish_item, src_db, trg_db)
# iterate over the yaml file and moves data


def create_data_table(trg_db, src_db, p_src_table, p_trg_table, partition_column):
    if not trg_db.table_exists(p_trg_table):
        logging.info("Table don't exixts so Creating: {}".format(p_trg_table))
        v_create_sql=None
        v_idx_sql=None
        # if src_db.dbtype in ['POSTGRES','CITUS']:
        #     v_create_sql, v_idx_sql = src_db.get_create_table_cli(
        #         p_src_table, p_trg_table, gen_fk=False)
        # else: 
        v_create_sql  = src_db.get_create_table_sqlalchemy(  p_src_table,trg_db)
        v='COLLATE "SQL_Latin1_General_CP1_CI_AS"'
        v_create_sql=str(v_create_sql).replace(p_src_table,p_trg_table).replace(v,'')
        src_db.print_tables([p_src_table.split('.')[-1]])
        trg_db.execute(v_create_sql,catch_exception=False)
        
        trg_db.execute(v_idx_sql)
        trg_db.commit()
       
        trg_db.execute(
            'ALTER TABLE {0} OWNER to operational_dba;'.format(p_trg_table))
        trg_db.execute("""select create_distributed_table('{table_name}','{field_name}'); """.format(
            table_name=p_trg_table, field_name=partition_column))
        trg_db.commit()


def create_work_table(trg_db, migration_item):
    pprint.pprint(migration_item)
    work_table = migration_item['db']['target_db']['work_table']
    # print(work_table)
    if trg_db.table_exists(work_table) == False:
        # print(WORK_TBL_SQL.format(work_table))
        trg_db.execute(WORK_TBL_SQL.format(work_table))
        trg_db.execute('commit;')
        trg_db.execute(
            'ALTER TABLE {0} OWNER to operational_dba;'.format(work_table))
# iterate over the yaml file and moves data


def add_work(trg_db, src_db, migration_list):
    import hashlib
    m = hashlib.md5()
    for migration_item in migration_list:

        table = migration_item['migration_params']

        insert_sql = """Insert into {0}(src_sql_hash, src_sql, trg_table  )
                values {1} ON CONFLICT (src_sql_hash) DO NOTHING;"""
        values = []

        work_table = migration_item['db']['target_db']['work_table']
        # print('querying source db for ids....then breaking')
        # import pprint
        # pprint.pprint(migration_item)
        v_batch_method = table.get('batch_method', None)

        if v_batch_method is None:
            print("need to pull from db")
        elif table['batch_method'] == 'ALL':
            sql = migration_item['source_sql']
            m.update(sql)
            data_val = []
            data_val.append(m.hexdigest())
            data_val.append(sql)
            data_val.append(migration_item['target_table'])

            values_sql = "('" + "','".join(data_val) + "')"
            values.append(values_sql)

        elif table['batch_method'] == 'COLUMN':

            ids = src_db.query(migration_item['chunk_sql'])
            if len(ids) == 0:
                raise Exception("{}: Return 0 Records".format(migration_item['chunk_sql']))
            for id in ids:
                v_id = id[0]
                sql = migration_item['source_sql'] + \
                    ' where {}={}'.format(migration_item['column'], v_id)
                m.update(sql)

                data_val = []
                data_val.append(m.hexdigest())
                data_val.append(sql)
                data_val.append(migration_item['target_table'])

                values_sql = "('" + "','".join(data_val) + "')"
                values.append(values_sql)
        else:
            raise Exception("Unknown batch_method in Yaml")

        insert_sql = insert_sql.format(work_table, ',\n'.join(values))

        trg_db.execute(insert_sql)

# close out a file that has completed processing
def finish_work(trg_db, work_table, pk):
    sql = ("""UPDATE {0}
                SET
                end_time=now()
                WHERE src_sql_hash ='{1}'
            """).format(work_table, pk)

    trg_db.execute(sql)
    trg_db.commit()


def get_work(trg_db, work_table):
            # to ensure we lock 1 row to avoid race conditions
    sql = ("""BEGIN;
                UPDATE {0}
                SET
                pid={1},
                start_time=now()

                WHERE pid is null and src_sql_hash in
                (select src_sql_hash
                    FROM {0}
                    WHERE pid is null limit 1
                     );
            END;
            """).format(work_table, os.getpid())

    trg_db.execute(sql)
    trg_db.commit()
    sql = "select * from {0} where start_time is not null and end_time is null and pid='{1}'".format(
        work_table, os.getpid())
    x = trg_db.query(sql)

    if len(x) == 0:
        return None
    return x


def set_log_level(debug_level):
    if debug_level == 'DEBUG':
        logging.setLevel(lg.DEBUG)
    if debug_level == 'INFO':
        logging.setLevel(lg.INFO)
    if debug_level == 'WARN':
        logging.setLevel(lg.WARN)
    if debug_level == 'ERROR':
        logging.setLevel(lg.ERROR)


def get_src_trg_db(publish_item, proc_num=0):
    import pprint
    source_db = None
    target_db = None
    source_host = None
    target_host = None
     
    appname = publish_item.get('appname', 'py_publish') + "_" + str(proc_num)
    yaml_db =publish_item.get('db', None)
    source_db = yaml_db['source_db']

    src_db = db_utils.DB(host=source_db['host'],
                                        port=source_db['port'],
                                        database=source_db['db'],
                                        dbschema=source_db['schema'],
                                        userid=source_db['userid'],
                                        password=os.environ.get(source_db['password_envir_var'],None),
                                        dbtype=source_db['type'],
                                        appname=appname)
    
    target_db = yaml_db['target_db']
    trg_db= None
    trg_db = db_utils.DB(host=target_db['host'],
                                        port=target_db['port'],
                                        database=target_db['db'],
                                        dbschema=target_db['schema'],
                                        userid=target_db['userid'],
                                        password=os.environ.get(target_db['password_envir_var'],None),
                                        dbtype=target_db['type'],
                                        appname=appname)

    # sys.exit()
    return src_db, trg_db

    
def mp_do_work(publish_item, proc_num, return_dict=None):

    src_db, trg_db = get_src_trg_db(publish_item, proc_num)
    
    work_table = publish_item['db']['target_db']['work_table']
    x = get_work(trg_db, work_table)

    while x is not None:

        v_pk = x[0][0]
        v_source_sql = x[0][1]
        v_target_table = x[0][2]
        truncate_target = publish_item.get('truncate_target', False)
        set_state(trg_db, work_table, v_pk, 'PID: {}'.format(os.getpid()))
        load_status = move_item(publish_item, src_db, trg_db, v_target_table, v_source_sql,
                                src_yaml=publish_item, Truncate=truncate_target, pk=v_pk, work_table=work_table)
        set_state(trg_db, work_table, v_pk,
                  'Rows Moved: {}'.format(load_status))
        finish_work(trg_db, work_table, v_pk)

        sql = publish_item.get(
            'post_load_per_tbl_sql', None) or return_dict.get(v_target_table, None)

        if sql is not None:
            return_dict[v_target_table] = {'table_name': v_target_table, 'sql': sql}
        x = get_work(trg_db, work_table)


def mp_query(publish_item, rs, proc_num, return_dict):
    def get_conn(publish_item):
        pb = copy.deepcopy(publish_item)
        pb['appname'] = 'py_idx_count'
        src_db, dummy = get_src_trg_db(pb, proc_num)
        return src_db
    src_db = get_conn(publish_item)

    for i, a in enumerate(rs):
        if i == proc_num:
            v_schema, v_tbl, v_idx_name, v_cols, v_is_pk = a

            if v_is_pk == False:

                cnt = src_db.get_a_value(
                    "select count(distinct concat({})) from {}.{}""".format(v_cols, v_schema, v_tbl))
                #print(v_schema, v_tbl, v_idx_name, v_cols, v_is_pk, proc_num)
                #print("             ProceNum:", i, cnt)
                return_dict[i] = cnt


def run_pre_sql_action(src_db, trg_db, publish_item):
    trg_pre_action_sql = publish_item.get("trg_pre_action_sql", None)
    if trg_pre_action_sql is not None:
        for sql in trg_pre_action_sql:
            sql = recurse_replace_yaml(sql, publish_item)
            trg_db.execute(sql,catch_exception=False)
            trg_db.commit()


# run through the yaml and replace embedded params
def pre_process_yaml(yaml_file):
    # yaml_file = os.path.abspath(yaml_file)
    yaml_data=None
    with open(yaml_file,'r') as f:
        yaml_data = yaml.full_load(f)
    log_level = args.log_level
    mig_list = []
    for yaml_obj in yaml_data:
        item = recurse_replace_yaml(yaml_obj, yaml_obj)
        mig_list.append(item)
        for i in item.keys():
            j = item[i]
            if isinstance(j, dict):

                j = recurse_replace_yaml(j, j)

            elif isinstance(j, list):

                j = recurse_replace_yaml(j, yaml_obj)

            elif isinstance(j, str):

                j = recurse_replace_yaml(j, yaml_obj)

    #pprint.pprint(mig_list)
     
    return mig_list


def process_yaml(yaml_data, args):
    # yaml_file = os.path.abspath(yaml_file)
    # yaml_data = yaml.load(open(yaml_file))

    log_level = args.log_level
    mapping_counter = 0
    source_db = None
    target_db = None
    source_host = None
    target_host = None
    # try:
    if 1 == 1:
        for publish_item in yaml_data:
            publish_item = recurse_replace_yaml(publish_item, publish_item)
            mapping_counter += 1
            delivery_type = publish_item['delivery_type']
            enabled = publish_item.get('enabled', True)
            set_log_level(publish_item.get('debug_level', log_level))
            yaml_target_db=publish_item['db']['target_db']
            skipped = False

            if delivery_type == DELIVERY_SCHEMA and enabled:

                src_db, trg_db = get_src_trg_db(publish_item)

                if args.show_index:
                    pprint.pprint(src_db.get_schema_index())
                if args.show_col_stats:
                    pprint.pprint(src_db.get_schema_col_stats())
                if args.count_index:
                    rs = (src_db.get_schema_index())

                    # variables visible to all spawn processes
                    manager = mp.Manager()
                    return_dict = manager.dict()
                    multi_process(
                        mp_query, [publish_item, rs], len(rs), return_dict)
                    # print(return_dict)

                if args.show_index or args.count_index or args.show_col_stats:
                    break
                v_trg_schema = yaml_target_db['schema']
                if (yaml_target_db.get('create_target_schema', False) and not trg_db.schema_exists(v_trg_schema)):
                    trg_db.execute("create role operational_dba")
                    trg_db.execute('Create schema {} authorization operational_dba'.format(v_trg_schema),catch_exception=False)
                    if trg_db._dbtype=='CITUS':
                        sql = "SELECT run_command_on_workers($cmd$ Create schema {} authorization operational_dba; $cmd$);".format(v_trg_schema)
                        trg_db.execute(sql,catch_exception=False)

                run_pre_sql_action(src_db, trg_db, publish_item)
                work_table = publish_item['db']['target_db']['work_table']
                create_work_table(trg_db, publish_item)
                # divide_work(trg_db,publish_item)
                if not trg_db.has_record('select 1 from {}'.format(work_table)):
                    plan_work(publish_item, src_db, trg_db)
                import datetime

                manager = mp.Manager()
                return_dict = manager.dict()
                num_proccess = publish_item.get('num_process', 1)
                multi_process(mp_do_work, [publish_item], num_proccess, return_dict)

                for t in return_dict.keys():

                    for sql in return_dict[t]['sql']:
                        v_sql = recurse_replace_yaml(sql, return_dict[t])
                        trg_db.execute(v_sql)
                        # pprint.pprint(return_dict['table_name'])

                        # pprint.pprint(return_dict.keys())

            else:
                skipped = True
                logging.warning(
                    "SKIPPING: Config Item did not have any matching DELIVERY_TYPE: {}".format(delivery_type))
                logging.debug("\t{}".format(publish_item))
            if not skipped:
                logging.info(
                    "Delivery Type: {} Completed".format(delivery_type))
                logging.info("Target Table: {} Completed".format(
                    publish_item.get('target_table', 'NONE')))

    # except Exception as e:
    #     logging.error("Error During YAML Processing: {}\n Error Message: {}".format(yaml_file, e))
    #     logging.error("Error Occured on item: {}:\n{}".format(mapping_counter, publish_item))
    #     traceback.print_exc()


def multi_process(funct, list_params, max_cores, p_return_dict=None):
    import copy
    process_list = []

    c = mp.cpu_count()
    if c > max_cores:
        c = max_cores
    for proc_num, i in enumerate(range(0, c)):
        list_p = copy.copy(list_params)
        list_p.append(proc_num)
        list_p.append(p_return_dict)
        e = mp.Process(target=funct, args=tuple(list_p))
        process_list.append(e)
        logging.info("Start Process : {}".format(i))

        e.start()

    # p.join()
    # iterate over each process to complete
    for i, proc in enumerate(process_list):
        proc.join()
        logging.info("Process Done: {} PID: {}".format(i, proc.pid))


def parse_cli_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--yaml', default=None, help='path to yaml file')
    parser.add_argument('--show_index', action='store_true',
                        help='Prints out all the index in the schema')
    parser.add_argument('--show_col_stats', action='store_true',
                        help='Prints out cardinality of analyzeed columns')
    parser.add_argument('--count_index', action='store_true',
                        help='Prints out counts of all index NB...VERY SLOW')
    parser.add_argument('--log_level', default='DEBUG',
                        help='Default Log Level')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    # process_list = []
    args = parse_cli_args()
    # multi process here for now
    # process_yaml(args.yaml, args.log_level)
    yaml_file = os.path.abspath(args.yaml)
    yaml_data=None
    with open(yaml_file,'r') as f:
        yaml_data = yaml.full_load(f)
    logging.info('Read YAML file: \n\t\t{}'.format(yaml_file))

    yaml_dict = pre_process_yaml(yaml_file)

    process_yaml(yaml_dict, args)
