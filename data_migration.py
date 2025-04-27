import os
import time
import hvac
import json
import urllib3
import requests
import subprocess
import mysql.connector
from multiprocessing import Pool, SimpleQueue
from dotenv import load_dotenv


urllib3.disable_warnings()
"""
    Load VAULT access
"""
load_dotenv('.env_db')

CONFIG_JSON = "migration.json"
V_TMP = ".vault"
RUNNER_FLAG_DIR = "/tmp"
DELAY_BEFORE_CHECKSUM = 0


# Gchat
TITLE = 'MySQL Migration Manager'
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


def get_datetime_column_position(mycurr, database, table):
    sql = "SELECT ORDINAL_POSITION-1 FROM information_schema.columns where table_schema='{}' and table_name='{}' and COLUMN_TYPE in ('datetime', 'timestamp')".format(
        database, table)
    mycurr.execute(sql)

    return mycurr


def get_table_column(mycurr, database, table):
    sql_table_header = "SELECT group_concat(concat('\''',column_name,'\''')) FROM information_schema.columns where table_schema='{}' and table_name='{}' and extra not in ('VIRTUAL GENERATED') order by ordinal_position asc".format(
        database, table)
    mycurr.execute(sql_table_header)

    return mycurr


def get_table_column_values(mycurr, database, table):
    sql_stmt = "SELECT group_concat(concat('\''',if(COLUMN_TYPE='datetime','%s','%s'),'\''')) FROM information_schema.columns where table_schema='{}' and table_name='{}' order by ordinal_position asc".format(
        database, table)
    mycurr.execute(sql_stmt)

    return mycurr


def get_max_id(mycurr, table, column, column_date="", min_date=""):
    if min_date == "" and column_date == "":
        sql_max = "SELECT MAX({}) FROM {}".format(column, table)
    else:
        sql_max = "SELECT MAX({}) FROM {} WHERE {} <= '{}'".format(
            column, table, column_date, min_date)

    mycurr.execute(sql_max)

    return mycurr


def get_min_id(mycurr, table, column, column_date="", min_date=""):
    if min_date == "" and column_date == "":
        sql_min = "SELECT MIN({}) FROM {}".format(column, table)
    else:
        sql_min = "SELECT MIN({}) FROM {} WHERE {} >= '{}'".format(
            column, table, column_date, min_date)

    mycurr.execute(sql_min)

    return mycurr


def fetch_data_source(
        mycurr,
        table,
        header_res,
        column_key,
        last_id,
        chunk_size):
    sql_fetch = "SELECT {} FROM {} WHERE {} > {} LIMIT {}".format(
        header_res, table, column_key, last_id, chunk_size)
    mycurr.execute(sql_fetch)

    return mycurr


def update_row(
        myconn,
        source_curr,
        source_table,
        target_table,
        header_res,
        value_res,
        column_key,
        update_res):
    update_res = ",".join(str(x) for x in update_res)
    value_res = value_res.replace("'", "")
    header_res = header_res.replace("'", "")

    sql_source = "SELECT {} FROM {} WHERE {} IN ({})".format(
        header_res, source_table, column_key, update_res)
    source_curr.execute(sql_source)
    data_res = source_curr.fetchall()

    try:
        mycurr = myconn.cursor()
        sql_upd_target = "REPLACE INTO {} ({}) VALUES({})".format(
            target_table, header_res, value_res)
        mycurr.executemany(sql_upd_target, data_res)
        myconn.commit()
        res = [True, int(int(mycurr.rowcount) / 2)]
    except mysql.connector.Error as e:
        print(e)

    return res


def migrate_data(myconn, header_res, value_res, data_res, table):
    value_res = value_res.replace("'", "")
    header_res = header_res.replace("'", "")
    try:
        mycurr = myconn.cursor()
        sql_migrate = "INSERT IGNORE INTO {} ({}) VALUES({})".format(
            table, header_res, value_res)
        mycurr.executemany(sql_migrate, data_res)
        myconn.commit()
        res = [True, mycurr.rowcount]

        return res
    except mysql.connector.Error as e:
        print(e)
        return False


def migrate_data_within_db(
        myconn,
        header_res,
        source_table,
        target_table,
        column_key,
        last_id,
        chunk_size):
    header_res = header_res.replace("'", "")
    try:
        mycurr = myconn.cursor()
        sql_stmt = "INSERT IGNORE INTO {} ({}) SELECT {} FROM {} WHERE {} > {} LIMIT {}".format(
            target_table, header_res, header_res, source_table, column_key, last_id, chunk_size)
        mycurr.execute(sql_stmt)
        myconn.commit()
        res = [True, mycurr.rowcount]

        return res
    except mysql.connector.Error as e:
        print(e)
        return False


def check_runner_flag(runner_flag):
    ops_comm = "ls {} | wc -l".format(runner_flag)
    ops_run = subprocess.run(
        ops_comm,
        shell=True,
        encoding='utf-8',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    if int(ops_run.stdout.strip()) == 1:
        return False
    else:
        return True


def checksum_table(
        source_curr,
        source_table,
        target_curr,
        target_table,
        header_res,
        column_key,
        min_id,
        method="BIT_XOR"):
    header_res = header_res.replace("'", "")

    sql_source = "SELECT COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} >= {};".format(
        method, header_res, source_table, column_key, min_id)
    source_curr.execute(sql_source)

    sql_target = "SELECT COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} >= {};".format(
        method, header_res, target_table, column_key, min_id)
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
            min_id,
            method="BIT_AND")
    elif method == "BIT_AND" and str(res_source) == str("0") and str(res_target) == str("0"):
        checksum_table(
            source_curr,
            source_table,
            target_curr,
            target_table,
            header_res,
            column_key,
            min_id,
            method="BIT_OR")
    else:
        if res_source == res_target:
            return True
        else:
            return False


def checksum_heuristical(
        source_curr,
        source_table,
        target_curr,
        target_table,
        header_res,
        column_key,
        min_id,
        method="BIT_XOR"):
    res = []
    header_res = header_res.replace("'", "")

    sql_source = "SELECT {}, COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} >= {} GROUP BY {} ORDER BY 1 ASC".format(
        column_key, method, header_res, source_table, column_key, min_id, column_key)
    source_curr.execute(sql_source)

    sql_target = "SELECT {}, COALESCE(LOWER(CONV({}(CAST(CRC32(concat({})) AS UNSIGNED)), 10, 16)), 0) FROM {} WHERE {} >= {} GROUP BY {} ORDER BY 1 ASC".format(
        column_key, method, header_res, target_table, column_key, min_id, column_key)
    target_curr.execute(sql_target)

    res_source = source_curr.fetchall()
    res_target = target_curr.fetchall()

    count_row_source = int(source_curr.rowcount)
    count_row_target = int(target_curr.rowcount)

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


def execute_data_migration(json_data):

    action = "execute data migration"

    is_active = json_data['is_active']
    is_autochecksum = json_data['is_autochecksum']
    is_switched = json_data['is_switched']
    source_db = json_data['source_db']
    source_endpoint = json_data['source_endpoint']
    source_table = json_data['source_table']
    target_db = json_data['target_db']
    target_endpoint = json_data['target_endpoint']
    target_table = json_data['target_table']
    column_key = json_data['column_key']
    chunk_size = json_data['chunk_size']
    min_date_period = json_data['min_date_period']
    column_date = json_data['column_date']
    row_auto_update = json_data['row_auto_update']

    runner_flag = "{}/{}-{}_{}-{}.run".format(
        RUNNER_FLAG_DIR, source_db, source_table, target_db, target_table)

    run_flag = check_runner_flag(runner_flag)

    if is_active:
        # open connection
        if run_flag:
            try:

                try:
                    ops_comm = "touch {}".format(runner_flag)
                    ops_run = subprocess.run(
                        ops_comm,
                        shell=True,
                        encoding='utf-8',
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
                except subprocess.CalledProcessError as e:
                    print(e)

                db_access = get_db_access()
                db_user = db_access[0]
                db_pass = db_access[1]

                try:
                    source_conn = mysql_connector(
                        source_endpoint, db_user, db_pass, source_db)
                    source_conn.ping(True)
                    source_curr = source_conn.cursor(buffered=True)
                except mysql.connector.Error as e:
                    if e.errno == 1045:
                        get_db_access(True)
                    else:
                        print(e)

                res_source_table_column = get_table_column(
                    source_curr, source_db, source_table).fetchone()[0]
                res_fetch_column_values = get_table_column_values(
                    source_curr, source_db, source_table).fetchone()[0]

                try:
                    target_conn = mysql_connector(
                        target_endpoint, db_user, db_pass, target_db)
                    target_conn.ping(True)
                    target_curr = target_conn.cursor(buffered=True)
                except mysql.connector.Error as e:
                    if e.errno == 1045:
                        get_db_access(True)
                    else:
                        print(e)

                if target_table != "" and column_key != "":

                    target_table_max_id = get_max_id(
                        target_curr, target_table, column_key).fetchone()[0]
                    if target_table_max_id is None:
                        last_id = get_max_id(
                            source_curr,
                            source_table,
                            column_key,
                            column_date,
                            min_date_period).fetchone()[0]
                    else:
                        last_id = int(target_table_max_id)

                    """
                    if source and target are within the same db,
                    we can just directly insert the table without buffer
                    """
                    if source_db == target_db and source_endpoint == target_endpoint:
                        if source_table == target_table:
                            x = '```target_table cannot be the same as source_table```'
                            print(x)
                            return
                        else:
                            action = "Migrate data within db"
                            res_migrate_data = migrate_data_within_db(
                                source_conn,
                                res_source_table_column,
                                source_table,
                                target_table,
                                column_key,
                                last_id,
                                chunk_size)
                            if int(res_migrate_data[1]) == 0:
                                ranged = "[ {} : {} ] to [ {} : {} ]".format(
                                    column_key, int(last_id), column_key, int(last_id))
                            else:
                                ranged = "[ {} : {} ] to [ {} : {} ]".format(column_key, int(
                                    last_id), column_key, int(last_id) + int(res_migrate_data[1]))

                            if res_migrate_data[0]:
                                msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Range : {} \n Info : {} Record/s inserted successfully".format(
                                    source_db, source_endpoint, source_table, target_db, target_endpoint, target_table, ranged, res_migrate_data[1])
                                print(msg)
                                gchat_post_messages(TITLE, action, msg)
                            else:
                                msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error : ```Something is wrong during migrating data within db. Please Check log.```".format(
                                    source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                                print(msg)
                                gchat_post_messages(TITLE, action, msg, False)
                    else:
                        action = "Migrate data to another db"
                        res_fetch_data_source = fetch_data_source(
                            source_curr,
                            source_table,
                            res_source_table_column,
                            column_key,
                            last_id,
                            chunk_size).fetchall()
                        res_migrate_data = migrate_data(
                            target_conn,
                            res_source_table_column,
                            res_fetch_column_values,
                            res_fetch_data_source,
                            target_table)
                        if int(res_migrate_data[1]) == 0:
                            ranged = "[ {} : {} ] to [ {} : {} ]".format(
                                column_key, int(last_id), column_key, int(last_id))
                        else:
                            ranged = "[ {} : {} ] to [ {} : {} ]".format(column_key, int(
                                last_id), column_key, int(last_id) + int(res_migrate_data[1]))

                        if res_migrate_data[0]:
                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Range : {} \n Info : {} Record/s inserted successfully".format(
                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table, ranged, res_migrate_data[1])
                            print(msg)
                            gchat_post_messages(TITLE, action, msg)
                        else:
                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error : ```Something is wrong during migrating data to another db. Please Check log.```".format(
                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                            print(msg)
                            gchat_post_messages(TITLE, action, msg, False)

                    if is_autochecksum:
                        time.sleep(DELAY_BEFORE_CHECKSUM)
                        print('***   ***   ***')

                        """
                        Checksum table
                        """
                        target_conn.reconnect()
                        source_conn.reconnect()

                        if is_switched:
                            res_min_id = get_min_id(
                                target_curr, target_table, column_key, column_date, min_date_period)
                        else:
                            res_min_id = get_min_id(
                                target_curr, target_table, column_key)

                        min_id = res_min_id.fetchone()[0]

                        res_check_sum_table = checksum_table(
                            source_curr,
                            source_table,
                            target_curr,
                            target_table,
                            res_source_table_column,
                            column_key,
                            min_id)
                        if res_check_sum_table:
                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Info : Table synced ".format(
                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                            print(msg)
                            gchat_post_messages(TITLE, action, msg)
                        else:
                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error : ```Table is not in synced yet.```".format(
                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                            print(msg)
                            gchat_post_messages(TITLE, action, msg, False)

                            if row_auto_update:
                                if int(res_migrate_data[1]) == 0:
                                    print('***   ***   ***')
                                    msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n info : Running heuristical check and auto update row/s".format(
                                        source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                                    print(msg)
                                    gchat_post_messages(TITLE, action, msg, False)

                                    res_checksum_heuristical = checksum_heuristical(
                                        source_curr,
                                        source_table,
                                        target_curr,
                                        target_table,
                                        res_source_table_column,
                                        column_key,
                                        min_id,
                                        method="BIT_XOR")
                                    res_update_row = update_row(
                                        target_conn,
                                        source_curr,
                                        source_table,
                                        target_table,
                                        res_source_table_column,
                                        res_fetch_column_values,
                                        column_key,
                                        res_checksum_heuristical)
                                    if res_update_row[0]:
                                        print('***   ***   ***')
                                        msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Update : {} \n Info : {} Record/s updated successfully".format(
                                            source_db, source_endpoint, source_table, target_db, target_endpoint, target_table, res_checksum_heuristical, res_update_row[1])
                                        print(msg)
                                        gchat_post_messages(TITLE, action, msg)

                                        print('***   ***   ***')

                                        res_check_sum_table = checksum_table(
                                            source_curr,
                                            source_table,
                                            target_curr,
                                            target_table,
                                            res_source_table_column,
                                            column_key,
                                            min_id)
                                        if res_check_sum_table:
                                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Info : Table synced ".format(
                                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                                            print(msg)
                                            gchat_post_messages(TITLE, action, msg)
                                        else:
                                            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error : ```Table is not in synced yet.```".format(
                                                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
                                            print(msg)
                                            gchat_post_messages(
                                                TITLE, action, msg, False)

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

                target_curr.close()
                target_conn.close()

                source_curr.close()
                source_conn.close()

                try:
                    ops_comm = "rm -f {}".format(runner_flag)
                    ops_run = subprocess.run(
                        ops_comm,
                        shell=True,
                        encoding='utf-8',
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE)
                except subprocess.CalledProcessError as e:
                    print(e)

            except mysql.connector.Error as e:
                print(e)
        else:
            msg = "Source : {}@{}/{} \n Target : {}@{}/{} \n Error : ```Migration is in progress.```".format(
                source_db, source_endpoint, source_table, target_db, target_endpoint, target_table)
            gchat_post_messages(TITLE, action, msg, False)


def main():
    """
    main function
    """
    with open(CONFIG_JSON) as json_file:
        json_data = json.load(json_file)

    # Multiprocess
    pool = Pool()

    # Execute data migration Tasks
    pool.map(execute_data_migration, json_data)

    # Close Pool
    pool.close()


if __name__ == '__main__':
    action = '*** STARTING DATA MIGRATION ***'
    print("{}\n{}\n*********".format(action, time.ctime()))
    gchat_post_messages(TITLE, action, time.ctime())

    # Execute Main
    main()

    action = '*** FINISH DATA MIGRATION ***'
    print("{}\n{}\n*********".format(action, time.ctime()))
    gchat_post_messages(TITLE, action, time.ctime())
