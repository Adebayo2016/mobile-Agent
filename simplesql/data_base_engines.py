import mysql.connector
import sqlite3
import psycopg2
from .sql_exceptions.my_exceptions import DbConnectionError


class MysqlClient:
    def __init__(self, **fields):
        try:
            self.db_connection = mysql.connector.connect(**fields)
            print(("--" * 5) + ' Database connection successful ' + ("--" * 5))
            self.db_info = self.db_connection.get_server_info()
            print("Connected to MySQL Server : ", self.db_Info)

        except Exception as e:
            print(' Database connection unsuccessful ')
            raise DbConnectionError(e)
        else:
            self.cursor = self.db_connection.cursor()
            self.tag = ('%s',), ' = %s', ' = %s '


class PostgresClient:
    def __init__(self, **fields):
        try:
            self.db_connection = psycopg2.connect(**fields)
            print(("--" * 5) + ' Database connection successful ' + ("--" * 5))
            self.db_info = self.db_connection.get_dsn_parameters()
            print("Connected to Postgres Server version \n", self.db_Info)

        except Exception as e:
            print(' Database connection unsuccessful ')
            raise DbConnectionError(e)

        else:
            self.cursor = self.db_connection.cursor()
            self.tag = ('%s',), ' = %s', ' = %s '


class SqliteClient:
    def __init__(self, **fields):
        try:
            self.db_connection = sqlite3.connect(**fields)
            print(("--" * 5) + ' Database connection successful ' + ("--" * 5))
            self.db_info = sqlite3.version
            print("Connected to Sqlite3 Server version:", self.db_info)

        except Exception as e:
            print(' Database connection unsuccessful ')
            raise DbConnectionError(e)

        else:
            self.cursor = self.db_connection.cursor()
            self.tags = ('?',), ' = ?', ' = ? ','=?'
