import platform, sys, os, argparse, base64, logging
from datetime import datetime

import pandas as pd             # pip install pandas
import snowflake.connector      # pip install snowflake-connector-python
import oracledb                 # pip install python-oracledb
import snowflake.connector.errors

from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent                  # Back from Run folder to Root DEVOPS_INF
lclsetup = root/f"lclsetup"
sys.path.append(str(root))
sys.path.append(str(lclsetup))

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup ,inf 


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

# Internal Components for handling Oracle and Snowflake
# Per Thick Client requirements: https://python-oracledb.readthedocs.io/en/latest/user_guide/troubleshooting.html#dpy-4011
# trying to this and INIT_ORACLE_CLIENT call to thick client libraries:
# https://python-oracledb.readthedocs.io/en/latest/user_guide/initialization.html#enablingthick
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


def connect_to_db(srcdb):
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


def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--src', help='The source connection from dbsetup passed as env/key/db [/schema]', required=True)
    parser.add_argument('-o','--output', help='Output directory', required=False)
    parser.add_argument('-t','--tables', help='Comma separated list of tables to extract', required=False)
    parser.add_argument('--sf', action='store_true', help='If set, load tables into SF')
    parser.add_argument('--ddl', action='store_true', help='If set, generate table DDL from oracle into ddl folder')
    parser.add_argument('--ddl_only', action='store_true', help='If true, only extract the DDL and do nothing else')
    parser.add_argument('--debug', action='store_true', help='If set, display additional data to help with troubleshooting')
    parser.add_argument('--push', action='store_true', help='If set, push CSV files from output directory into SF')
    args = parser.parse_args()

    args.src = args.src.strip('/')
    if args.src.count("/") == 3:
        args.srcenv, args.srckey, args.src_db, args.src_schema  = args.src.split('/')
    elif args.src.count('/') == 2:
        args.srcenv, args.srckey, args.src_db = args.src.split('/')
        args.src_schema = None
    else:
        args.src_db, args.src_schema = None, None
        args.srcenv, args.srckey = args.src.split('/', 1 )

    return args


def main():
    args = process_args()
    print(":: STARTING MAIN")
    print(f" >> Arguments: {args}")
    print(f"  > Source: {args.srcenv}/{args.srckey}/{args.src_db}/{args.src_schema}")

    print(f"Determining database type from dbsetup...")
    srcdb = dbsetup.config['env'][args.srcenv][args.srckey]
    print(f" >> dbsetup says: {srcdb}")
    # srccon = dblib.DB(srcdb, log = args.log, port = srcdb.get('port',''))
    cursor = connect_to_db(srcdb)
    if srcdb.get('dbtype', '') == 'oracle':
        cursor.execute('select sysdate from dual')
        print( cursor.fetchall() )
    if srcdb.get('dbtype', '') == 'snowflake':
        cursor.execute('select current_timestamp()')
        print( cursor.fetchall() )
    

if __name__ == "__main__":
    setup_logging()
    init_oracle_client()
    # with patch('oracledb.init_oracle_client'):
    #     test_init_oracle_client()
    main()

"""
hq8627osx:DEVOPS_INF>intel  
hq8627osx:DEVOPS_INF>arch  
i386
hq8627osx:DEVOPS_INF>pyenv activate ora_envx86                                                                                                                       main

(ora_envx86) hq8627osx:DEVOPS_INF>python test-ora.py  
"""
# python testlogin.py --src tt/E21BSE/E21TRUBIS     
# python testlogin.py --src moen/dev
# python testlogin.py --src mlock/RDMLCKI    
# python testlogin.py --src dev/fbronze/     
# python testlogin.py --src dev/funct/     
