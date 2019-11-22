import json
import os

from src.sql_file_executor import SqlFileExecutor
from src.playground_db_connection import PlaygroundDbConnector


def get_success_response(event):
    body = {
        "message": "The file has been executed with success",
        "input": event
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response


def exec_marketing_voucher_v2(event, context):
    conn = PlaygroundDbConnector.get_db_connection()

    sql_executor = SqlFileExecutor(conn)
    sql_executor.execute_sql_file(os.environ['LAMBDA_TASK_ROOT'] + '/sql/marketing_voucher_v2.sql')
    return get_success_response(event)
