import getpass, platform, csv, os, argparse, base64, logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import concurrent.futures
# from sqlalchemy import create_engine

import pandas as pd
import oracledb
import snowflake.connector

logging.basicConfig( level=logging.INFO, format="%(processName)s - %(levelname)s - %(message)s" )

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
    init_oracle_client()
    user = "RAC_ACCNT"
    # dsn = "diatmlckidb01.tmlc.ras:1522/DMLCKI"
    dsn = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=diatmlckidb01.tmlc.ras)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=DMLCKI)(INSTANCE_NAME=DMLCKI)))"
    # pw = getpass.getpass( f"Enter a password for {user}: ")
    pw ="kVysh_Ast04r"

    con = oracledb.connect( user=user, password=pw, dsn=dsn )
    print("Connected.")
    print("DSN: ", con.dsn)
    return con
    # engine = create_engine( f'oracle+cx_oracle://user:{user}:{pw}@{dsn}')
    # return engine

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

def upload_file_to_snowflake(sfcon, df, file_path, table_name, year, slicer):
    file_format = """file_format = (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create a unique table name for each partition
    phys_table = f"{table_name}_{year}_{slicer}".replace('.', '_')

    print(f"\t{now} ==> Uploading {phys_table} to SF account: PLAYGROUND.RAC_ACCNT")
    with sfcon.cursor() as cur:
        cur.execute("USE SCHEMA RAC_ACCNT")
        cur.execute("USE WAREHOUSE PLAYGROUND_WH")
        
        cur.execute(f"CREATE STAGE IF NOT EXISTS oracle_stage FILE_FORMAT = (TYPE='CSV')")

        cur.execute(f"PUT file://{file_path} @~/oracle_stage auto_compress=true;")

        type_mapping = {
            'object': 'text',
            'int64': 'number',
            'float64': 'float',
            'datetime64[ns]': 'timestamp',
        }

        columns = ', '.join([f"{col} {type_mapping[str(df[col].dtype)]}" for col in df.columns])
        create_sql = f"CREATE OR REPLACE TABLE {phys_table} ({columns});"
        print(f"--> {create_sql}")
        cur.execute(create_sql)

        sql = f"""COPY INTO PLAYGROUND.RAC_ACCNT.{phys_table} from @~/oracle_stage/{phys_table} {file_format} on_error='continue'; """
        print(f'---> {sql}')
        cur.execute(sql)

    print(f"\t... copied into {phys_table}")



# Start of actual data movement processing for this script
def read_tables_from_csv(file_path):
    print(f"=== Reading tables from csv: {file_path} ")
    tables = []

    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip the header row
        for row in reader:
            schema_table = row[0]
            date_column = row[1] if len(row) > 1 else None
            slicer_column = row[2] if len(row) > 2 else None
            tables.append({
                "schema_table": schema_table,
                "date_column": date_column,
                "slicer_column": slicer_column
            })
    
    print(f"... found {len(tables)} tables")
    return tables

def extract_tables_to_csv(table_details, output_dir, year, segment=None):
    schema, table_name = table_details['schema_table'].split(".")
    date_column = table_details['date_column']
    slicer_column = table_details['slicer_column']
    conditions = []
    
    if date_column:
        year_condition = f"< '2020'" if year == 'PRE_2020' else f"= '{year}'"
        conditions.append(f"TO_CHAR({date_column}, 'YYYY') {year_condition}")
    if slicer_column and segment is not None:
        conditions.append(f"MOD({slicer_column}, 4) = {segment}")
    
    where_clause = " AND ".join(conditions)
    where_clause = f" WHERE {where_clause}" if conditions else ""
    year_str = 'PRE_2020' if year == 'PRE_2020' else str(year)
    
    schema_dir = os.path.join(output_dir, schema, year_str)
    os.makedirs(schema_dir, exist_ok=True)
    segment_label = f"{year_str}_{segment+1:02}" if segment is not None else year_str
    segment_str = f"{segment+1:02}" if segment is not None else ""
    csv_file = os.path.join(schema_dir, f"{table_name}_{segment_label}.csv")
    
    query = f"SELECT * FROM {schema}.{table_name} {where_clause}"
    logging.info(f"Extracting {schema}.{table_name} for {segment_label} \n\tUSING: {query}") 
    
    try:
        con = connect_to_oracle()
        df = pd.read_sql(query, con)
    except Exception as e:
        logging.error(f"Error: {e}")
        return None
    
    df.to_csv(csv_file, index=False)
    logging.info(f"... written to {csv_file}")
    return csv_file, year, segment_str

def process_table(table_details, year, args, con_snowflake):
    logging.info(f"Processing {table_details} for {year}... ")
    if table_details['slicer_column']:
        with ProcessPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(extract_tables_to_csv, table_details, args.output, year, segment) for segment in range(4)]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result and args.sf:
                    csv_file, year_segment, slicer_segment = result
                    df = pd.read_csv(csv_file)
                    upload_file_to_snowflake(con_snowflake, df, csv_file, table_details['schema_table'], year_segment, slicer_segment)
    else:
        result = extract_tables_to_csv(table_details, args.output, year)
        if result and args.sf:
            csv_file, year_segment, _ = result
            df = pd.read_csv(csv_file)
            upload_file_to_snowflake(con_snowflake, df, csv_file, table_details['schema_table'], year_segment, None)


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
    tables = read_tables_from_csv(args.tables)
    current_year = datetime.now().year

    if args.sf:
        con_snowflake = connect_to_snowflake()

    for table_details in tables:
        if table_details['date_column']:
            # Process for each year if there's a date column
            years = ['PRE_2020'] + list(range(2020, current_year + 1))
            for year in years:
                process_table(table_details, year, args, con_snowflake)
        else:
            # Process only once if there's no date column
            process_table(table_details, None, args, con_snowflake)



if __name__ == "__main__":
    main()    



# python test-oracle-para.py -t tables.csv -o outputs 
# python test-oracle-para.py -t tables.csv -o outputs --sf
