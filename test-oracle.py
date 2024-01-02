import oracledb
import getpass, platform, csv, os, argparse, base64
import pandas as pd
import snowflake.connector
import datetime from datetime

def decode_password( encoded_password ):
    return base64.b64decode( encoded_password ).decode()

def init_oracle_client():
    # Enable the thick client since NNE is enabled in Oracle
    d = None  # default suitable for Linux
    if platform.system() == "Darwin" and platform.machine() == "x86_64":   # macOS
        d = os.environ.get("HOME")+("/Downloads/instantclient_19_21")
    elif platform.system() == "Windows":
        d = r"C:\temp\instantclient_19_21"

    oracledb.init_oracle_client( lib_dir= d )

def connect_to_oracle():
    user = "RAC_ACCNT"
    # dsn = "diatmlckidb01.tmlc.ras:1522/DMLCKI"
    dsn = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=diatmlckidb01.tmlc.ras)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=DMLCKI)(INSTANCE_NAME=DMLCKI)))"
    pw = getpass.getpass( f"Enter a password for {user}: ")

    con = oracledb.connect( user=user, password=pw, dsn=dsn )
    print("Connected.")
    print("DSN: ", con.dsn)
    return con

def connect_to_snowflake( ):
    password = decode_password( b'NDI0N1hTMnNub3dmbGFrZSE=' )
    sfcon = snowflake.connector.connect(
        user="DBFUNCT",
        password=password,
        account="fbhs_gpg.east-us-2.azure",
        warehouse="DATALOAD_DEV",
        database="PLAYGROUND",
        authentication="snowflake"
    ) 
    return sfcon

def upload_file_to_snowflake( sfcon, df, file_path, table_name):
    file_format=f"""file_format =  (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  """

    print(f"\t==> Uploading to SF account: PLAYGROUND.RAC_ACCNT")
    with sfcon.cursor() as cur:
        cur.execute("USE SCHEMA RAC_ACCNT")
        cur.execute("USE WAREHOUSE PLAYGROUND_WH")
        
        cur.execute( f"CREATE STAGE IF NOT EXISTS oracle_stage FILE_FORMAT = (TYPE='CSV')" )

        cur.execute(f"PUT file://{file_path} @~/oracle_stage auto_compress=true;")

        phys_table = table_name.split('.')[-1]
        print(f" >>> {phys_table} <<<")

        type_mapping = {
            'object':'text',
            'int64':'number',
            'float64':'float',
            'datetime64[ns]':'timestamp',
        }
        # Generate a list of columns and create an empty table:
        # columns = ', '.join([ f"{col} TEXT" for col in df.columns])
        columns = ', '.join([ f"{col} {type_mapping[str(df[col].dtype)]}" for col in df.columns])
        create_sql = f"CREATE OR REPLACE TABLE {phys_table} ({columns});"
        print(f"--> {create_sql}")
        cur.execute( create_sql )

        # cur.execute(f"CREATE OR REPLACE TABLE {phys_table} as select *  from @oracle_stage")
        sql = f"""COPY INTO PLAYGROUND.RAC_ACCNT.{phys_table} from @~/oracle_stage/{phys_table} {file_format} on_error='continue'; """
        print( f'---> {sql}')
        cur.execute( sql )
    print(f"\t... copied into {table_name}")

def read_tables_from_csv( file_path ):
    print(f"=== Reading tables from csv: {file_path} ")
    with open( file_path, 'r') as f:
        reader = csv.reader( f )
        tables = list( reader )
    print(f"... found {len(tables)} tables")
    return tables

def extract_tables_to_csv( con, table, output_dir ):
    schema, table_name = table[0].split(".")
    schema_dir = os.path.join( output_dir, schema )
    os.makedirs( schema_dir, exist_ok= True )
    csv_file = os.path.join( schema_dir, f"{table_name}.csv")

    print(f"\t{datetime.datetime.now()}=> Extracting {schema}.{table_name}")
    query = f"select * from {table[0]}"
    df = pd.read_sql( query, con )

    print(f"... writing to {schema_dir}")
    df.to_csv( csv_file, index=False )

    return df, csv_file

def process_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument('-f','--file', help='CSV file path', required=True )
    parser.add_argument('-o','--output', help='Output directory', required=True )
    parser.add_argument('-t','--tables', help='Comma separated list of tables to extract', required=True )
    parser.add_argument('--sf', action='store_true', help='If set, load tables into SF')

    args = parser.parse_args()
    return args


def main():
    args = process_args()
    con_snowflake = False

    init_oracle_client()
    con = connect_to_oracle()
    tables = read_tables_from_csv( args.tables )

    if args.sf:
        con_snowflake = connect_to_snowflake()
    
    for table in tables:
        df, csv_file = extract_tables_to_csv( con, table, args.output )
        if args.sf:
            upload_file_to_snowflake( con_snowflake, df, csv_file, table[0] )

if __name__ == "__main__":
    main()    



# python test-oracle.py -t tables.csv -o outputs 
# python test-oracle.py -t tables.csv -o outputs --sf