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


def sync_shop(event, context):
    sync_table(os.environ['TABLE_SHOP'])
    return get_success_response(event)


def sync_order(event, context):
    sync_table_in_loop(os.environ['TABLE_ORDER'])
    return get_success_response(event)


def sync_product(event, context):
    sync_table_in_loop(os.environ['TABLE_PRODUCT'])
    return get_success_response(event)


def sync_business_category(event, context):
    sync_table(os.environ['TABLE_BUSINESS_CATEGORY'])
    return get_success_response(event)


def sync_product_business_category(event, context):
    sync_table(os.environ['TABLE_PRODUCT_BUSINESS_CATEGORY'])
    return get_success_response(event)


def sync_product_lang(event, context):
    sync_table_in_loop(os.environ['TABLE_PRODUCT_LANG'])
    return get_success_response(event)


def sync_manufacturer(event, context):
    sync_table(os.environ['TABLE_MANUFACTURER'])
    return get_success_response(event)


def sync_order_cart_rule(event, context):
    sync_table(os.environ['TABLE_ORDER_CART_RULE'])
    return get_success_response(event)


def sync_cart_rule(event, context):
    sync_table(os.environ['TABLE_CART_RULE'])
    return get_success_response(event)


def sync_order_detail(event, context):
    sync_table_in_loop(os.environ['TABLE_ORDER_DETAIL'])
    return get_success_response(event)


def sync_zaius_permission(event, context):
    sync_table_in_loop(os.environ['TABLE_ZAIUS_PERMISSION'])
    return get_success_response(event)


def sync_warehouse_supplier_order_detail(event, context):
    sync_table(os.environ['TABLE_WAREHOUSE_SUPPLIER_ORDER_DETAIL'])
    return get_success_response(event)


def sync_warehouse_supplier_order(event, context):
    sync_table(os.environ['TABLE_WAREHOUSE_SUPPLIER_ORDER'])
    return get_success_response(event)


def sync_supplier_order_history(event, context):
    sync_table(os.environ['TABLE_SUPPLIER_ORDER_HISTORY'])
    return get_success_response(event)


def sync_customer(event, context):
    sync_table_in_loop(os.environ['TABLE_CUSTOMER'])
    return get_success_response(event)


def sync_address(event, context):
    sync_table_in_loop(os.environ['TABLE_ADDRESS'])
    return get_success_response(event)


def sync_country(event, context):
    sync_table(os.environ['TABLE_COUNTRY'])
    return get_success_response(event)


def sync_pack(event, context):
    sync_table_in_loop(os.environ['TABLE_PACK'])
    return get_success_response(event)
