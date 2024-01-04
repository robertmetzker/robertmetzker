import getpass, platform, csv, os, argparse, base64, logging
from datetime import datetime
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import oracledb
import snowflake.connector


def setup_logging():
    logging.basicConfig(
        level = logging.INFO,
        format="%(processName)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )



def decode_password(encoded_password):
    return base64.b64decode(encoded_password).decode()

# Internal Components for handling Oracle and Snowflake
def init_oracle_client():
    d = None  # default suitable for Linux

    if platform.system() == "Darwin" and platform.machine() == "x86_64":   # macOS
        d = os.environ.get("HOME")+("/Downloads/instantclient_19_21")
    elif platform.system() == "Windows":
        d = r"C:\temp\instantclient_19_21"
    oracledb.init_oracle_client(lib_dir=d)


def connect_to_oracle():
    user = "RAC_ACCNT"
    dsn = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=diatmlckidb01.tmlc.ras)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=DMLCKI)(INSTANCE_NAME=DMLCKI)))"
    pw = "kVysh_Ast04r"

    con = oracledb.connect(user=user, password=pw, dsn=dsn)
    logging.info("Connected to Oracle.")
    return con

def connect_to_snowflake():
    password = decode_password(b'NDI0N1hTMnNub3dmbGFrZSE=')
    sfcon = snowflake.connector.connect(
        user="DBFUNCT",
        password=password,
        account="fbhs_gpg.east-us-2.azure",
        warehouse="DATALOAD_DEV",
        database="PLAYGROUND",
        authentication="snowflake"
    ) 

    logging.info("Connected to Snowflake.")
    return sfcon

def upload_file_to_snowflake(sfcon, df, file_path, table_name, year, slicer):
    print(f">>>> starting UPLOAD_FILE_TO_SNOWFLAKE for {table_name}")

    file_format = """file_format = (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')"""
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    phys_table_parts = [table_name.replace('.', '_')]

    if year:
        phys_table_parts.append(str(year))
    if slicer:
        phys_table_parts.append(slicer)
    phys_table = "_".join(phys_table_parts)
    print(f"\t{now} ==> Uploading {phys_table} to SF account: PLAYGROUND.RAC_ACCNT")
    logging.info(f"\t{now} ==> Uploading {phys_table} to SF account: PLAYGROUND.RAC_ACCNT")
    
    with sfcon.cursor() as cur:
        cur.execute("USE SCHEMA RAC_ACCNT")
        cur.execute("USE WAREHOUSE PLAYGROUND_WH")
        cur.execute(f"CREATE STAGE IF NOT EXISTS oracle_stage FILE_FORMAT = (TYPE='CSV')")
        cur.execute(f"PUT file://{file_path} @~/oracle_stage auto_compress=true;")

        type_mapping = {'object': 'text', 'int64': 'number', 'float64': 'float', 'datetime64[ns]': 'timestamp'}
        columns = ', '.join([f"{col} {type_mapping[str(df[col].dtype)]}" for col in df.columns])
        create_sql = f"CREATE OR REPLACE TABLE {phys_table} ({columns});"
        print(f"--> {create_sql}")
        logging.info(f"--> {create_sql}")
        cur.execute(create_sql)

        sql = f"""COPY INTO PLAYGROUND.RAC_ACCNT.{phys_table} from @~/oracle_stage/{phys_table} {file_format} on_error='continue'; """
        print(f'---> {sql}')
        logging.info(f'---> {sql}')
        cur.execute(sql)
    print(f"\t... copied into {phys_table}: {sql}")
    logging.info(f"\t... copied into {phys_table}")

def read_tables_from_csv(file_path):
    print(">>> READING TABLES FROM CSV")
    print(f"=== Reading tables from csv: {file_path} ")
    tables = []

    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        next(reader, None)  # Skip the header row
        for row in reader:
            if row[0].startswith('#'): 
                continue
            else:
                schema_table = row[0]
                date_column = row[1] if len(row) > 1 else None
                slicer_column = row[2] if len(row) > 2 else None
                granularity = row[3] if len(row) > 3 else 'Y'
                tables.append({"schema_table": schema_table, "date_column": date_column, "slicer_column": slicer_column, "granularity": granularity})

    print(f"... found {len(tables)} tables")
    logging.info(f"... found {len(tables)} tables")
    return tables

def extract_and_upload(args, table_details, output_dir, con_snowflake):
    print(f">>>> STARTING EXTRACT AND UPLOAD: {table_details}")

    schema, table_name = table_details['schema_table'].split(".")
    date_column = table_details['date_column']
    slicer_column = table_details['slicer_column']
    granularity = table_details['granularity']

    current_year = datetime.now().year
    current_month = datetime.now().month

    def build_where_clause(base_clause, slicer_segment):
        slicer_clause = f" AND MOD({slicer_column},4) = {slicer_segment}" if slicer_column else ""
        return f"{base_clause}{slicer_clause}"

    if date_column:
        combinations = generate_monthly_segments(2021, current_year) if granularity =='YM' else generate_yearly_segments(2021, current_year) 

        for combination in combinations:
            if granularity == 'YM'  and int(combination) > int(f"{current_year}{current_month:02}"):
                continue  #ignore future months
            base_clause = f"WHERE TO_CHAR({date_column}, 'YYYY{'' if granularity=='Y' else 'MM'}') = '{combination}'"
        
            with ProcessPoolExecutor(max_workers=1) as executor:
                futures = []
                for slicer_segment in range(4):
                    where_clause = build_where_clause(base_clause, slicer_segment)
                    print(f"\t==> Queueing job for {table_name} > {combination}_{slicer_segment} > {where_clause} ")
                    future = executor.submit(process_extraction, args, schema, table_name, where_clause, f"{combination}_{slicer_segment}", output_dir, table_details['schema_table'] )
                    futures.append(future)
                    # process_extraction( schema, table_name, where_clause, f"{combination}_{slicer_segment}", output_dir, table_details['schema_table'], con_snowflake)
                concurrent.futures.wait(futures)                

            for future in futures:
                if future.exception() is not None:
                    print(f"!!!! Exception: {str(future.exception()) }")
                    logging.error("A Task encountered an Exception")

    else:
        process_extraction( args, schema, table_name, '', '', output_dir, table_details['schema_table'] )

def process_extraction( args, schema, table_name, where_clause, filter, output_dir, full_table_name ):
    print(f">>>>> PROCESS_EXTRACTION for {table_name} > {str(filter)} > {where_clause}")

    setup_logging()
    try:
        con = connect_to_oracle()
        con_snowflake = connect_to_snowflake()

        filter_dir = filter[:4] if len(filter) > 4  else filter     # Parse out the year portion
        schema_dir = os.path.join( output_dir, schema, filter_dir )
        os.makedirs(schema_dir, exist_ok=True)

        year, slicer = (filter.split('_') + [None])[:2] if '_' in filter else (filter, None)

        csv_file_name = f"{table_name}_{filter}.csv" if filter else f"{table_name}.csv"
        csv_file = os.path.join(schema_dir, csv_file_name )

        query = f"SELECT * FROM {schema}.{table_name} {where_clause}"
        print(f"Extracting {schema}.{table_name} for {csv_file_name} \n\tUSING: {query}") 
        logging.info(f"Extracting {schema}.{table_name} for {csv_file_name} \n\tUSING: {query}") 

        df = pd.read_sql(query, con)
        df.to_csv( csv_file, index=False)
        print(f"... written to {csv_file}")
        logging.info(f"... written to {csv_file}")

        if args.sf:
            upload_file_to_snowflake(con_snowflake, df, csv_file, full_table_name, year, slicer)

    except Exception as e:
        print(f"Error in process_extraction: {e}")
        logging.error(f"Error in process_extraction: {e}")
        return
    # finally:
    #     con.close()
    #     con_snowflake.close()



def generate_yearly_segments(start_year, end_year):
    print(f">>>> GENERATING YEARLY SEGMENTS BETWEEN {start_year} and {end_year}")
    return [ str(year) for year in range(start_year, end_year+1)]

def generate_monthly_segments(start_year, end_year):
    print(f">>>> GENERATING MONTHLY SEGMENTS BETWEEN {start_year} and {end_year}")
    return [ f"{year}{month:02}" for year in range(start_year, end_year+1) for month in range(1,13)]


def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o','--output', help='Output directory', required=True)
    parser.add_argument('-t','--tables', help='Comma separated list of tables to extract', required=True)
    parser.add_argument('--sf', action='store_true', help='If set, load tables into SF')
    args = parser.parse_args()
    return args

def main():
    print(">> STARTING MAIN")
    args = process_args()
    init_oracle_client()
    tables = read_tables_from_csv(args.tables)
    con_snowflake = connect_to_snowflake() if args.sf else None

    for table_details in tables:
        logging.info(f"--- PROCESSING: {table_details}")
        extract_and_upload(args, table_details, args.output, con_snowflake)
        logging.info(f"--- COMPLETED: {table_details}")

if __name__ == "__main__":
    main()


# python test-oracle-para.py -t tables.csv -o outputs 
# python test-oracle-para.py -t tables.csv -o outputs --sf
