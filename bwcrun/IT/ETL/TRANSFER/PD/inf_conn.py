import os
import cx_Oracle
import pyodbc
import snowflake.connector

def get_db_connection(env, db_type):
    # Get the credentials from the db_config dictionary
    credentials = db_config[env][db_type]

    # Open a connection based on the db_type
    if db_type == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(credentials['host'], credentials['port'], credentials['database'])
        connection = cx_Oracle.connect(credentials['user'], credentials['password'], dsn_tns)
    elif db_type == 'SQLServer':
        connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + credentials['host'] + ';DATABASE=' + credentials['database'] + ';UID=' + credentials['user'] + ';PWD=' + credentials['password']
        connection = pyodbc.connect(connection_string)
    elif db_type == 'Snowflake':
        connection = snowflake.connector.connect(
            user=credentials['user'],
            password=credentials['password'],
            account=credentials['account'],
            warehouse=credentials['warehouse'],
            database=credentials['database'],
            schema=credentials['schema']
        )
    else:
        print("Invalid database type")
        return None

    return connection