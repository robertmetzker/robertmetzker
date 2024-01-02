import getpass, platform, csv, os, argparse, base64
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import oracledb
import snowflake.connector

def decode_password( encoded_password ):
    return base64.b64decode( encoded_password ).decode()

## Internal Components for handling Oracle and Snowflake
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
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(f"\t{now} ==> Uploading to SF account: PLAYGROUND.RAC_ACCNT")
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


# Start of actual data movement processing for this script
def read_tables_from_csv(file_path):
    print(f"=== Reading tables from csv: {file_path} ")

    tables = []
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            schema_table, date_column, slicer_column = row
            tables.append({
                "schema_table": schema_table,
                "date_column": date_column,
                "slicer_column": slicer_column
            })
    
    print(f"... found {len(tables)} tables")
    return tables

def extract_tables_to_csv(con, table_details, output_dir, year, segment):
    schema, table_name = table_details['schema_table'].split(".")
    slicer_column = table_details['slicer_column']
    date_column = table_details['date_column']
    
    # Handling for pre-2020 and post-2020 data
    year_condition = f"< '2020'" if year == 'pre-2020' else f"= '{year}'"
    
    schema_dir = os.path.join(output_dir, schema, year)
    os.makedirs(schema_dir, exist_ok=True)
    segment_label = 'pre-2020' if year == 'pre-2020' else f"{year}_{segment+1:02}"
    csv_file = os.path.join(schema_dir, f"{table_name}_{segment_label}.csv")

    query = f"SELECT * FROM {schema_table} WHERE TO_CHAR({date_column}, 'YYYY') {year_condition} AND MOD({slicer_column}, 4) = {segment}"
    
    print(f"=> Extracting {schema}.{table_name} for {segment_label}")
    try:
        df = pd.read_sql(query, con)
    except Exception as e:
        print(f"Error: {e}")
        return None

    df.to_csv(csv_file, index=False)
    print(f"... written to {csv_file}")
    return csv_file


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
    init_oracle_client()
    con = connect_to_oracle()
    tables = read_tables_from_csv(args.tables)

    current_year = datetime.now().year
    years = ['pre-2020'] + list(range(2020, current_year + 1))

    if args.sf:
        con_snowflake = connect_to_snowflake()

    for table_details in tables:
        for year in years:
            with ProcessPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(extract_tables_to_csv, con, table_details, args.output, year, segment) for segment in range(4)]
                for future in concurrent.futures.as_completed(futures):
                    csv_file = future.result()
                    if csv_file and args.sf:
                        df = pd.read_csv(csv_file)
                        upload_file_to_snowflake(con_snowflake, df, csv_file, table_details['schema_table'])



if __name__ == "__main__":
    main()    



# python test-oracle.py -t tables.csv -o outputs 
# python test-oracle.py -t tables.csv -o outputs --sf
