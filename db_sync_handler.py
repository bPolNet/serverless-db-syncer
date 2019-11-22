import os
import json

from src.sql_file_executor import SqlFileExecutor
from src.playground_db_connection import PlaygroundDbConnector

SOURCE_HOST = os.environ['SOURCE_HOST']
SOURCE_USER = os.environ['SOURCE_USER']
SOURCE_PASS = os.environ['SOURCE_PASS']
SOURCE_DB = os.environ['SOURCE_DB']


def sync_table(table_name):
    print('starting dump: ' + table_name)
    dumpcmd = os.environ['LAMBDA_TASK_ROOT'] + "/bin/mysqldump -h " + SOURCE_HOST + " -u " + SOURCE_USER + " -p" + \
              SOURCE_PASS + " " + SOURCE_DB + " " + table_name + " --lock-tables=false > /tmp/" + table_name + ".sql"
    os.system(dumpcmd)

    conn = PlaygroundDbConnector.get_db_connection()
    sql_file_executor = SqlFileExecutor(conn)
    sql_file_executor.execute_sql_file('/tmp/' + table_name + '.sql')


def sync_table_in_loop(table_name):
    row_limit = 100000
    insert_limit = 30
    insert_count = 0
    loop_number = 0
    while loop_number == 0 or insert_count >= insert_limit:
        print('starting dump: ' + table_name + ' loop: ' + str(loop_number))

        if loop_number == 0:
            dumpcmd = os.environ[
                          'LAMBDA_TASK_ROOT'] + "/bin/mysqldump -h " + SOURCE_HOST + " -u " + SOURCE_USER + " -p" + \
                      SOURCE_PASS + " " + SOURCE_DB + " " + table_name + \
                      " --lock-tables=false " \
                      "--where=\"1=1 LIMIT " + str(row_limit * loop_number) + ", " + str(row_limit) + "\" > /tmp/" + \
                      table_name + ".sql"
        else:
            dumpcmd = os.environ[
                          'LAMBDA_TASK_ROOT'] + "/bin/mysqldump -h " + SOURCE_HOST + " -u " + SOURCE_USER + " -p" + \
                      SOURCE_PASS + " " + SOURCE_DB + " " + table_name + \
                      " --lock-tables=false " \
                      "--skip-add-drop-table " \
                      "--no-create-info " \
                      "--where=\"1=1 LIMIT " + str(row_limit * loop_number) + ", " + str(row_limit) + "\" > /tmp/" + \
                      table_name + ".sql"
        os.system(dumpcmd)

        conn = PlaygroundDbConnector.get_db_connection()
        sql_file_executor = SqlFileExecutor(conn)
        insert_count = sql_file_executor.execute_sql_file('/tmp/' + table_name + '.sql')

        loop_number = loop_number + 1


def get_success_response(event):
    body = {
        "message": "The db sync has been executed with success",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


def sync_daily(event, context):
    for sync_table in os.environ['TABLE_LIST_DAILY'].split(","):
        sync_table_in_loop(sync_table)
    return get_success_response(event)


def sync_hourly(event, context):
    for sync_table in os.environ['TABLE_LIST_HOURLY'].split(","):
        sync_table_in_loop(sync_table)
    return get_success_response(event)
