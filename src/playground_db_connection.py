import os

import pymysql

TARGET_HOST = os.environ['TARGET_HOST']
TARGET_USER = os.environ['TARGET_USER']
TARGET_PASS = os.environ['TARGET_PASS']
TARGET_DB = os.environ['TARGET_DB']


class PlaygroundDbConnector:
    @staticmethod
    def get_db_connection():
        conn = pymysql.connect(TARGET_HOST,
                               user=TARGET_USER,
                               passwd=TARGET_PASS,
                               db=TARGET_DB,
                               connect_timeout=5,
                               cursorclass=pymysql.cursors.DictCursor)

        return conn
