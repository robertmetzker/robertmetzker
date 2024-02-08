import csv, base64, logging, os, re, sys, snowflake.connector, multiprocessing
from datetime import datetime
import pandas as pd
import oracledb         # import cx_Oracle

from contextlib import closing
from joblib import Parallel, delayed
from snowflake.connector.pandas_tools import write_pandas

from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent                  # Back from Run folder to Root DEVOPS_INF
lclsetup = root/f"lclsetup"
sys.path.append(str(root))
sys.path.append(str(lclsetup))


class DBSetup:
    def __init__(self, config, dbtype='snowflake'):
        self.dbtype = dbtype
        self.account = config.get('account', '') 
        self.user = config.get('user', '')
        self.password = config.get('password', '')
        self.database = config.get('database', '') 
        self.role = config.get('role', '')
        self.warehouse = config.get('warehouse', '')
        self.authenticator = config.get('authenticator', '')
        self.keep_alive = config.get('CLIENT_SESSION_KEEP_ALIVE', '')

def setup_logging():
    filename = f'logs/tests_conn_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    
   # Ensure the directory for the log file exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)

    logging.basicConfig(
        filename = filename,
        level = logging.INFO,
        format="%(processName)s - %(levelname)s - %(message)s",
    )

def decode_password(encoded_password):
    if isinstance(encoded_password, bytes):
        encoded_password = encoded_password.decode()
    # Add padding if necessary
    missing_padding = len(encoded_password) % 4
    if missing_padding:
        encoded_password += '=' * (4 - missing_padding)
    try:
        decoded_password = base64.b64decode(encoded_password).decode('utf-8')
    except UnicodeDecodeError:
        try:
            decoded_password = base64.b64decode(encoded_password).decode('iso-8859-1')
        except UnicodeDecodeError:
            raise ValueError(f"Invalid encoding for password: {encoded_password}")
    return decoded_password


def init_oracle_client():
    d = None  # default suitable for Linux in case we move to an automated env.

    if platform.system() == "Darwin" and platform.machine() == "x86_64":   # macOS
        # d = os.environ.get("HOME")+("/Downloads/instantclient_19_21")
        d = os.environ.get("HOME")+("/instantclient")   # Changed since I had to build a custom Oracle Client for Apple Silicon
    elif platform.system() == "Windows":
        d = r"C:\temp\instantclient_19_21"
    try:
        oracledb.init_oracle_client(lib_dir=d)
    except oracledb.DatabaseError:
        print("Oracle connections are currently disabled.")

def connect_to_oracle( user, pw, host, port, service_name ):
    # dsn = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=dmaslorcl01)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=e21bse.masl10devsrv1)))"
    pw = decode_password(pw)

    # con = oracledb.connect(user=user, password=pw, dsn=dsn)
    dsn = host+':'+port+'/'+service_name
    print(f"\t- Connecting to Oracle: {dsn}")
    logging.info(f"\t- Connecting to Oracle: {dsn}")
    
    con = None  # Initialize the 'con' variable
    try:
        con = oracledb.connect(user=user, password=pw, dsn=dsn)
    except oracledb.DatabaseError as e:
        error_code = e.args[0].code
        if error_code == 12154:
            logging.error("!!! TNS issue detected. Please check your VPN connection.")
            print("TNS issue detected. Please check your VPN connection.")
        else:
            raise

    return con

def connect_to_snowflake(account, warehouse, database, user, password):
    password = decode_password(password)
    sfcon = None  # Initialize the 'sfcon' variable

    try:
        sfcon = snowflake.connector.connect(
            user=user, 
            password=password,
            account=account,
            warehouse=warehouse,
            database="PLAYGROUND",
            authentication="snowflake"
        )
        logging.info("Connected to Snowflake.")
    except snowflake.connector.errors.DatabaseError as e:
        error_code = e.errno
        if error_code == 250001:
            logging.error("!!! URL issue detected. Please check your Snowflake account URL.")
            print("URL issue detected. Please check your Snowflake account URL.")
    else:
            logging.error(f"An error occurred: {e}")
            raise

    return sfcon


def get_dbcon( srcdb ):
    '''
    Returns a database cursor object.  contained in object:
    config['dev']['rollback']
    {'account': 'fbhs_gpg.east-us-2.azure', 'user': 'robert.metzker@fbwinn.com', 'database': 'ROLLBACK', 'role': 'ACCOUNTADMIN', 'warehouse': 'WH_DATALOAD_PROD', 'authenticator': 'externalbrowser', 'CLIENT_SESSION_KEEP_ALIVE': True}    
    
    called by:  get_dbcon( srcdb 
    '''
    # config, env, db, schema  = pkg['config'], pkg['env'], pkg['db'], pkg.get('schema','PUBLIC')
    if srcdb.get('dbtype', '') == 'oracle':
        con = connect_to_oracle( srcdb.get('user', ''), srcdb.get('password', ''), srcdb.get('host', ''), srcdb.get('port', ''), srcdb.get('service_name', '') )
        if con:
            print(f" >> Connected to Oracle: {srcdb.get('dsn', '')}")
            sql =  "select to_char(sysdate,'YYYY-MM-DD HH24:MI:SS') from dual"
            cursor = con.cursor()
            cursor.execute( sql )
            results = cursor.fetchall()
            print(f" >> Connected to Oracle @{results[0][0]}")
        else:
            print(f"### No Oracle Connection was made.")

    elif srcdb.get('dbtype','') =='snowflake':
        con = connect_to_snowflake( srcdb.get('account', ''), srcdb.get('warehouse', ''), srcdb.get('database', ''), srcdb.get('user', ''), srcdb.get('password', ''))
        if con:
            print(f" >> Connected to Snowflake: {srcdb.get('account', '')}")
            print(f" >> Warehouse: {srcdb.get('warehouse', '')}")
            sql =  "select to_char(current_timestamp(),'YYYY-MM-DD HH24:MI:SS') as now"
            cursor = con.cursor()
            cursor.execute( sql )
            results = cursor.fetchall()
            print(f" >> Connected to Snowflake @{results[0][0]}")
        else:
            print("### No Snowflake Connection was made.")
    else:
        print(f" >> Unknown database type: {srcdb.get('dbtype', '')}")
        sys.exit(1)
    return cursor


def clean_columns( df ):
    """Clean column names to be Snowflake Compatible"""
    df.columns = [re.sub('[^A-Za-z0-9_]+', '_', col ) for col in df.columns]
    return df


def read_sql( filepath ):
    """Read file into"""
    with open( filepath, 'r', newline = '') as f:
        sql = f.readlines()
    return sql


def read_csv(path):
    # Ensure the file exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"No such file or directory: '{path}'")

    # Read the CSV file
    with open(path, 'r') as f:
        reader = csv.reader(f)
        data = list(reader)

    return data


def write_csv(path, data):
    # Ensure the directory structure exists
    os.makedirs(os.path.dirname(path), exist_ok=True)

    # Write the CSV file
    with open(path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(data)


def run_sql( all_args ):
    """Run SQL query against Snowflake connection"""
    srcdb, query = all_args
    cursor = get_dbcon( srcdb )

    # If running in SF, to prevent merges from failing when matching more than one row...
    # Alter the session and set ERROR_ON_NONDETERMINISTIC_MERGE = FALSE before running
    try:
        if len(query) == 2 and  'merge into' in query[1].lower():
            cursor.execute( "alter session set ERROR_ON_NONDETERMINISTIC_MERGE = FALSE" )
            cursor.execute( query[1] )
        else:
            cursor.execute( query )
    except Exception as e:
        error_msg = str(e).replace('\n','')
        return f'ERROR: ###### {query}\n{error_msg}'

    results = ( query, cursor.fetchall() )
    return results
    

def run_sql_parallel( all_args ):
    process_name, srcdb, sql_list = all_args
    print( f"\n> Connecting for {process_name} Parallel Run:")
    conn = get_dbcon( srcdb )
    queries = [(srcdb, sql) for sql in sql_list]

    pool_size = 5 if len( sql_list ) > 5 else 5
    pool = multiprocessing.Pool( processes =  pool_size )
    results = pool.map( run_sql, queries )

    # Prevent anything else from being submitted and close wait until all pool is closed
    pool.close()
    pool.join()

    return results


def run_sql_async( all_args ):
    """Run SQL Queries in Parallel"""
    srcdb, queries = all_args
    cur = get_dbcon( srcdb )

    if cur:
        qry_id,results = [],[]

        print(f"  => Running {len(queries)} queries...")
        for sql in queries:
            try:
                cur.execute_async(sql)
                qid = cur.sfqid
                qry_id.append(qid)
            except Exception as e:
                print(f"QID {qid} failed: {sql}. Error: {e}")

        print("\n Fetching...")
        for id in qry_id:
            cur.get_results_from_sfqid(id)
            results += cur.fetchall()

    return results


def extract_ddl_oracle( srcdb, schema, table_name ):
    """Extract the DDL for the given schema and table from Oracle.
    
    Connects to the configured Oracle database, runs a query to get the 
    DDL for the specified schema and table, writes the result to a DDL 
    file, and returns the DDL string.
    
    Args:
        schema (str): The schema name in Oracle.
        table_name (str): The table name in Oracle.
    
    Returns:
        str: The DDL string for the given schema and table.
    """
    cursor = get_dbcon( srcdb )
    ddl = ''
    if srcdb.get('dbtype', '') == 'oracle':
        ddl_sql = f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name}','{schema}') FROM DUAL"
    elif srcdb.get('dbtype','') =='snowflake':
        ddl_sql = f"select GET_DDL('table','{schema}.{table_name}')"
    print(f"\t<< GENERATING DDL for {schema}.{table_name}:{ddl_sql}")
    logging.info(f"\t<< GENERATING DDL for {schema}.{table_name}:{ddl_sql}")

    cursor.execute( ddl_sql )
    ddl = cursor.fetchone()[0]
    return str(ddl)

def get_column_data_types(srcdb, schema, table_name):
    """Gets the data types for columns in the given Oracle table."""
    print(f"\n\t>> Getting Datatypes for {schema}.{table_name}")
    logging.info(f"\n\t>> Getting Datatypes for {schema}.{table_name}")

    cursor = get_dbcon( srcdb )
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{table_name.upper()}'
    AND OWNER = '{schema.upper()}'
    ORDER BY COLUMN_ID
    """
    cursor.execute(query)
    col_data_types = {f"{schema}.{table_name}": {row[0]: row[1] for row in cursor.fetchall()}}

    return col_data_types


def generate_table_create_sql( srcdb, schema, table_name):
    """Generates a CREATE TABLE statement for the given Oracle table."""
    
    print(f"\n\t>> Generating CREATE TABLE for {schema}.{table_name}")
    logging.info(f"\n\t>> Generating CREATE TABLE for {schema}.{table_name}")
        
    try:
        cursor = get_dbcon( srcdb )
        query = f"""
        SELECT column_name, data_type, data_length, data_precision, data_scale, column_id
        FROM all_tab_columns
        WHERE table_name = '{table_name.upper()}' 
        AND owner = '{schema.upper()}'
        ORDER BY column_id
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        create_stmt = f"""CREATE TABLE {schema}.{table_name} (\n"""
    
        for row in rows:
            column_name = row[0]
            data_type = row[1]
            data_length = row[2]
            data_precision = row[3]
            data_scale = row[4]
            
            if data_type == 'DATE':
                data_type = 'TIMESTAMP'
            
            if data_type in ('NUMBER','FLOAT','BINARY_FLOAT','BINARY_DOUBLE'):
                if data_precision is not None and data_scale is not None:
                    create_stmt += f"\t {column_name} {data_type}({data_precision},{data_scale}),\n"
                elif data_precision is not None:
                    create_stmt += f"\t{column_name} {data_type}({data_precision}),\n"
                else:
                    create_stmt += f"\t{column_name} {data_type},\n"
            elif data_type in ('VARCHAR2','NVARCHAR2','CHAR','NCHAR'):
                create_stmt += f"\t{column_name} {data_type}({data_length}),\n"
            else:
                create_stmt += f"\t{column_name} {data_type},\n"
            
        create_stmt = create_stmt.rstrip(",\n") + "\n);"
        
    except:
        print(f"\t-- Error generating CREATE TABLE for {schema}.{table_name}")
        logging.info(f"\t-- Error generating CREATE TABLE for {schema}.{table_name}")
        create_stmt = None
        
    return create_stmt