import os
import time
import hvac
import json
import urllib3
import requests
import mysql.connector
from multiprocessing import Pool, SimpleQueue
from dotenv import load_dotenv

urllib3.disable_warnings()
"""
    Load VAULT access
"""
load_dotenv('.env_db')

CONFIG_JSON = "migration_checker.json"
V_TMP = ".vault"
RUNNER_FLAG_DIR = "/tmp"
DELAY_BEFORE_CHECKSUM = 0


# Gchat
TITLE = 'MySQL Migration Checker'
GCHAT_SPACE = ''


def gchat_post_messages(title, action, msg, state=True):
    if not state:
        action = "(!!!) {}".format(action)
    msg = "Date : {}\n{}".format(time.ctime(), msg)
    widget = {'textParagraph': {'text': msg}}
    res = requests.post(
        GCHAT_SPACE,
        json={
            'cards': [
                {
                    'header': {
                        'title': title,
                        'subtitle': action,
                    },
                    'sections': [{'widgets': [widget]}],
                }
            ]
        },
    )


def memorize(v_s, v_r):

    sr = 'v = "{},{}"'.format(v_s, v_r)

    try:
        with open(V_TMP, "wb") as f:
            f.write(sr.encode())
    except Exception as e:
        print(e)


def forget():
    v = os.path.exists(V_TMP)

    if v:
        os.remove(V_TMP)


def get_db_access(memo=False):

    if memo:
        forget()

    v = os.path.isfile(V_TMP)

    if not v:
        print('From Vault')
        VAULT_URL = os.getenv('VAULT_URL')
        VAULT_ROLE_ID = os.getenv('VAULT_ROLE_ID')
        VAULT_SECRET_ID = os.getenv('VAULT_SECRET_ID')
        VAULT_PATH = os.getenv('VAULT_PATH')
        VAULT_MOUNT_POINT = os.getenv('VAULT_MOUNT_POINT')
        VAULT_VERIFY = bool(os.getenv('VAULT_VERIFY'))

        if VAULT_VERIFY:
            vc = hvac.Client(url=VAULT_URL)
        else:
            vc = hvac.Client(url=VAULT_URL, verify=False)

        vc.auth.approle.login(VAULT_ROLE_ID, VAULT_SECRET_ID)
        is_connected = vc.is_authenticated()
        if is_connected:
            try:
                vc_res = vc.secrets.kv.v2.read_secret_version(
                    mount_point=VAULT_MOUNT_POINT, path=VAULT_PATH, raise_on_deleted_version=False)
                db_access = [
                    vc_res['data']['data']['db_retention_user'],
                    vc_res['data']['data']['db_retention_pass']]

                s = vc_res['data']['data']['db_retention_user']
                r = vc_res['data']['data']['db_retention_pass']

                memorize(s, r)

            except Exception as e:
                print(e)
        else:
            print('Vault is not available')
    else:
        load_dotenv(V_TMP)
        v = os.getenv('v')
        db_access = v.split(',')

    return db_access


def mysql_connector(endpoint, jumphost_user, jumphost_password, database):
    """
    Function for mysql connector to database server
    """
    mydb = mysql.connector.connect(
        host=endpoint,
        user=jumphost_user,
        passwd=jumphost_password,
        database=database,
        connection_timeout=300
    )

    return mydb


def get_table_column(mycurr, database, table):
    sql_table_header = "SELECT group_concat(concat('\''',column_name,'\''')) FROM information_schema.columns where table_schema='{}' and table_name='{}' order by ordinal_position asc".format(
        database, table)
    mycurr.execute(sql_table_header)

    return mycurr


def get_table_column_values(mycurr, database, table):
    sql_stmt = "SELECT group_concat(concat('\''',if(COLUMN_TYPE='datetime','%s','%s'),'\''')) FROM information_schema.columns where table_schema='{}' and table_name='{}' and extra not in ('VIRTUAL GENERATED') order by ordinal_position asc".format(
        database, table)
    mycurr.execute(sql_stmt)

    return mycurr


def get_min_id(mycurr, table, column, column_date="", min_date=""):
    if min_date == "" and column_date == "":
        sql_min = "SELECT MIN({}) FROM {}".format(column, table)
    else:
        sql_min = "SELECT MIN({}) FROM {} WHERE {} >= '{} 00:00:00' and {} <= '{} 23:59:59'".format(
            column, table, column_date, min_date, column_date, min_date)

    mycurr.execute(sql_min)

    return mycurr


def get_max_id(mycurr, table, column, column_date="", min_date=""):
    if min_date == "" and column_date == "":
        sql_max = "SELECT MAX({}) FROM {}".format(column, table)
    else:
        sql_max = "SELECT MAX({}) FROM {} WHERE {} >= '{} 00:00:00' and {} <= '{} 23:59:59'".format(
            column, table, column_date, min_date, column_date, min_date)
        
    try:
        mycurr.execute(sql_max)
    except Exception as e:
        print(e)

    return mycurr


def checksum_table(
        source_curr,
        source_table,
        target_curr,
        target_table,
        header_res,
        column_key,
        min_id,
        max_id,
        method="BIT_XOR"):
    sql_source = "SELECT COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} BETWEEN {} AND {};".format(
        method, header_res, source_table, column_key, min_id, max_id)
    source_curr.execute(sql_source)

    sql_target = "SELECT COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} BETWEEN {} AND {};".format(
        method, header_res, target_table, column_key, min_id, max_id)
    target_curr.execute(sql_target)

    res_source = source_curr.fetchone()[0]
    res_target = target_curr.fetchone()[0]

    if method == "BIT_XOR" and str(res_source) == str(
            "0") and str(res_target) == str("0"):
        checksum_table(
            source_curr,
            source_table,
            target_curr,
            target_table,
            header_res,
            column_key,
            min_id,max_id,
            method="BIT_AND")
    elif method == "BIT_AND" and str(res_source) == str("0") and str(res_target) == str("0"):
        checksum_table(
            source_curr,
            source_table,
            target_curr,
            target_table,
            header_res,
            column_key,
            min_id,max_id,
            method="BIT_OR")
    else:
        if res_source == res_target:
            return True
        else:
            return False


def checksum_heuristical(
        source_curr,
        source_repl_curr,
        source_table,
        target_curr,
        target_repl_curr,
        target_table,
        header_res,
        column_key,
        min_id,
        max_id,
        method="BIT_XOR"):
    
    res = []

    sql_source = "SELECT {}, COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} BETWEEN {} AND {} GROUP BY {} ORDER BY 1 ASC".format(
        column_key, method, header_res, source_table, column_key, min_id, max_id, column_key)
    source_repl_curr.execute(sql_source)

    sql_target = "SELECT {}, COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} BETWEEN {} AND {} GROUP BY {} ORDER BY 1 ASC".format(
        column_key, method, header_res, target_table, column_key, min_id, max_id, column_key)
    target_repl_curr.execute(sql_target)

    res_source = source_repl_curr.fetchall()
    res_target = target_repl_curr.fetchall()

    count_row_source = int(source_repl_curr.rowcount)
    count_row_target = int(target_repl_curr.rowcount)
    
    if count_row_source >= count_row_target:
        it_row = count_row_source
    else:
        it_row = count_row_target
    
    for data in range(it_row):
        rt_id = res_target[data][0]
        rs_id = res_source[data][0]
        rt_checksum = res_target[data][1]
        rs_checksum = res_source[data][1]

        if rt_checksum != rs_checksum:
            res.insert(data, rt_id)

    return res


def update_row(
        target_conn,
        source_curr,
        source_table,
        target_table,
        header_res,
        value_res,
        column_key,
        update_res):
    update_res = ",".join(str(x) for x in update_res)
    header_res = header_res.replace("'", "")

    sql_source = "SELECT {} FROM {} WHERE {} IN ({})".format(
        header_res, source_table, column_key, update_res)
    source_curr.execute(sql_source)
    data_res = source_curr.fetchall()

    try:
        mycurr = target_conn.cursor()
        sql_upd_target = "REPLACE INTO {} ({}) VALUES({})".format(
            target_table, header_res, value_res)
        mycurr.executemany(sql_upd_target, data_res)
        target_conn.commit()
        res = [True, int(int(mycurr.rowcount) / 2)]
    except mysql.connector.Error as e:
        print(e)
        
    return res


def checker(json_data):
    action="Data Synchronization check"
    source_db = json_data['source_db']
    source_endpoint = json_data['source_endpoint']
    source_endpoint_replica = json_data['source_endpoint_replica']
    source_table = json_data['source_table']
    target_db = json_data['target_db']
    target_endpoint = json_data['target_endpoint']
    target_endpoint_replica = json_data['target_endpoint_replica']
    target_table = json_data['target_table']
    column_key = json_data['column_key']
    column_date = json_data['column_date']
    min_date_period = json_data['min_date_period']
    max_date_period = json_data['max_date_period']
    chunk_size = json_data['chunk_size']
    is_active = json_data['is_active']
    is_heuristical = json_data['is_heuristical']
    row_auto_sync = json_data['row_auto_sync']

    is_checksum = True
    checksum_max_range = 0
    res_checksum_heuristical = []

    if is_active:
        if target_table != "" and column_key != "":
            db_access = get_db_access()
            db_user = db_access[0]
            db_pass = db_access[1]

            try:
                source_conn = mysql_connector(
                    source_endpoint, db_user, db_pass, source_db)
                source_conn.ping(True)
                source_curr = source_conn.cursor()
            except mysql.connector.Error as e:
                if e.errno == 1045:
                    get_db_access(True)
                else:
                    print(e)

            try:
                source_repl_conn = mysql_connector(
                    source_endpoint_replica, db_user, db_pass, source_db)
                source_repl_conn.ping(True)
                source_repl_curr = source_repl_conn.cursor()
            except mysql.connector.Error as e:
                if e.errno == 1045:
                    get_db_access(True)
                else:
                    print(e)

            try:
                target_conn = mysql_connector(
                    target_endpoint, db_user, db_pass, target_db)
                target_conn.ping(True)
                target_curr = target_conn.cursor()
            except mysql.connector.Error as e:
                if e.errno == 1045:
                    get_db_access(True)
                else:
                    print(e)
            
            try:
                target_repl_conn = mysql_connector(
                    target_endpoint_replica, db_user, db_pass, target_db)
                target_repl_conn.ping(True)
                target_repl_curr = target_repl_conn.cursor()
            except mysql.connector.Error as e:
                if e.errno == 1045:
                    get_db_access(True)
                else:
                    print(e)

            # check if data is sync
            min_target_curr = target_repl_conn.cursor()
            if min_date_period == "":
                res_target_min_id = get_min_id(min_target_curr, target_table, column_key).fetchone()[0]
            else:
                res_target_min_id = get_min_id(min_target_curr, target_table, column_key, column_date, min_date_period).fetchone()[0]

            target_conn.reconnect()
            max_target_curr = target_repl_conn.cursor()
            if max_date_period == "":
                res_target_max_id = get_max_id(max_target_curr, target_table, column_key).fetchone()[0]
            else:
                res_target_max_id = get_max_id(min_target_curr, target_table, column_key, column_date, max_date_period).fetchone()[0]

            if min_date_period == "" and max_date_period == "":
                res_source_max_id = get_max_id(source_repl_curr, source_table, column_key).fetchone()[0]
                max_id = res_source_max_id

                if res_source_max_id != res_target_max_id:
                    is_heuristical = False
                    msg = "Source and target {}s are out of sync [source : {} | target {}]. Please run the data migration.".format(column_key, res_source_max_id, res_target_max_id)
                    print(msg)
                    gchat_post_messages(TITLE, action, msg)
                    is_checksum = False
                    return
            else:
                max_id = res_target_max_id

            """
            """
            source_conn.reconnect()
            curr_source_table_column = source_repl_conn.cursor()
            res_source_table_column = get_table_column(
                curr_source_table_column, source_db, source_table).fetchone()[0]
            res_fetch_column_values = get_table_column_values(
                    curr_source_table_column, source_db, source_table).fetchone()[0]

            min_id = res_target_min_id
            i = 1
            while is_checksum:
                checksum_max_range = int(min_id) + int(chunk_size)
                
                if is_heuristical:
                    res_checksum_heuristical = checksum_heuristical(
                        source_curr,
                        source_repl_curr,
                        source_table,
                        target_curr,
                        target_repl_curr,
                        target_table,
                        res_source_table_column.replace(
                            "'",
                            ""),
                        column_key,
                        min_id, checksum_max_range)
                    
                    res_status = "SYNC" if not res_checksum_heuristical else "OUT OF SYNC !!!"

                    msg = "Iteration [{}] - {}/s : {} - {} | {} [{}] record/s".format(i, column_key, min_id, checksum_max_range, res_status, res_checksum_heuristical)
                    print(msg) 
                    print(res_checksum_heuristical)
                    """
                    if row_auto_sync and res_checksum_heuristical:
                        print("Synchronizing Data...")
                        msg = "Synchronizing Data for {}/s : {}".format(column_key, res_checksum_heuristical)
                        res_update_row = update_row(
                                target_conn,
                                source_repl_curr,
                                source_table,
                                target_table,
                                res_source_table_column.replace("'",""),
                                res_fetch_column_values.replace("'",""),
                                column_key,
                                res_checksum_heuristical)
                        if res_update_row[0]:
                            print('***   ***   ***')
                            print("{} \n {} Record/s updated successfully".format(res_checksum_heuristical, res_update_row[1]))
                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Update : {} \n Info : {} Record/s updated successfully".format(
                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table, res_checksum_heuristical, res_update_row[1])
                            gchat_post_messages(TITLE, action, msg)
                    """

                else:
                    res_check_sum_table = checksum_table(
                        source_curr,
                        source_table,
                        target_curr,
                        target_table,
                        res_source_table_column.replace(
                            "'",
                            ""),
                        column_key,
                        min_id, checksum_max_range)
                            
                    if not res_check_sum_table:
                        is_checksum = False

                min_id = checksum_max_range
                i += 1
                
                if min_id >= max_id:
                    if is_heuristical:
                        res_check_sum_table = True if not res_checksum_heuristical else False
                    is_checksum = False

            if res_check_sum_table:
                msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Info : Table is in synced ".format(
                    source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                print(msg)
                gchat_post_messages(TITLE, action, msg)
            else:
                if is_heuristical:
                    msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Info : ```Please check following [{}] in target_table : {}```".format(
                        source_db, source_endpoint, source_table, target_db, target_endpoint, target_table, column_key, res_checksum_heuristical)
                    print(msg)
                    gchat_post_messages(TITLE, action, msg, False)
                else:
                    msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error : ```Table is not in synced yet.```".format(
                        source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                    print(msg)
                    gchat_post_messages(TITLE, action, msg, False)
        else:
            if target_table == "":
                x = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error :```target_table cannot be empty```".format(
                    source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
            else:
                x = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error :```column_key cannot be empty```".format(
                    source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)

            print(x)
            gchat_post_messages(TITLE, action, x, False)
            return

def main():
    """
    main function
    """
    with open(CONFIG_JSON) as json_file:
        json_data = json.load(json_file)

    # with Pool(initializer=init_worker) as pool:
    # Multiprocess
    pool = Pool()

    # Execute data migration Tasks
    pool.map(checker, json_data)
    #checker(json_data)

    # Close Pool
    pool.close()


if __name__ == '__main__':
    action = '*** STARTING DATA MIGRATION CHECKER ***'
    print("{}\n{}\n*********".format(action, time.ctime()))
    gchat_post_messages(TITLE, action, time.ctime())

    # Execute Main
    main()

    action = '*** FINISH DATA MIGRATION CHECKER ***'
    print("{}\n{}\n*********".format(action, time.ctime()))
    gchat_post_messages(TITLE, action, time.ctime())
