"""
Script Name: Data Transfer from Oracle to Snowflake

Description:
This script facilitates the extraction of data from an Oracle database, processes the data,
and uploads it to Snowflake. It is designed to handle multiple tables, configurable via a CSV file.
The script supports uploading CSV files directly to Snowflake and processing data with specified granularity.

Usage:
Run the script with necessary command-line arguments to specify the operation mode:
- Extract and upload data from Oracle to Snowflake
- Directly push CSV files to Snowflake

The script uses environment variables for sensitive credentials and supports different operating systems
for Oracle client initialization.

Example Commands:
python ora2sf.py -t tables.csv -o outputs 
python ora2sf.py -t missed.csv -o outputs -p 4 --sf
python ora2sf.py -o outputs --sf --push --log-level DEBUG
python ora2sf.py -o outputs --sf --replace 
"""


import getpass, platform, csv, os, argparse, base64, glob, logging, warnings
import json, time
from datetime import datetime
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import oracledb
import snowflake.connector


def setup_logging(log_level='INFO'):
    script_name = os.path.basename(__file__).split('.')[0]
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"{script_name}_{timestamp}.log"

    logging_level = getattr(logging, log_level.upper(), None)
    if not isinstance(logging_level, int):
        raise ValueError(f'Invalid log level: {log_level}')

    logging.basicConfig(
        level=logging_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
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
    pw = 'a1Z5c2hfQXN0MDRy'.encode("ascii")
    pw = decode_password(pw)

    con = oracledb.connect(user=user, password=pw, dsn=dsn)
    # print(f"Connected to Oracle: {dsn}")
    logging.info(f"Connected to Oracle: {dsn}")
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


def push_csv_to_snowflake( args, sfcon):
    print(f">> PUSHING CSV FILES TO SNOWFLAKE")
    for root, dirs, files in os.walk( args.output ):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join( root, file )
                table_name = os.path.splitext(file)[0]
                df = pd.read_csv( file_path, low_memory=False )
                upload_file_to_snowflake( args, sfcon, df, file_path, table_name, None, None )

def create_stage_in_snowflake(sfcon, stage_name, file_format):
    sql = f"CREATE STAGE IF NOT EXISTS {stage_name} FILE_FORMAT = {file_format}"
    try:
        with sfcon.cursor() as cur:
            cur.execute(sql)
            logging.debug(f"Executed SQL: {sql}")
    except Exception as e:
        logging.error(f"Error executing SQL: {sql}, Error: {e}")


def upload_file_to_stage(sfcon, file_path, stage_name):
    sql = f"PUT file://{file_path} @~/{stage_name} auto_compress=true;"
    try:
        with sfcon.cursor() as cur:
            cur.execute(sql)
            logging.debug(f"Executed SQL: {sql}")
    except Exception as e:
        logging.error(f"Error executing SQL: {sql}, Error: {e}")


def create_table_from_stage(sfcon, df, phys_table, stage_name):
    type_mapping = {'object': 'text', 'int64': 'number', 'float64': 'float', 'datetime64[ns]': 'timestamp'}
    columns = ', '.join([f"{col} {type_mapping[str(df[col].dtype)]}" for col in df.columns])
    create_sql = f"CREATE OR REPLACE TABLE {phys_table} ({columns});"
    try:
        with sfcon.cursor() as cur:
            cur.execute(create_sql)
            logging.debug(f"Executed SQL: {create_sql}")
    except Exception as e:
        logging.error(f"Error executing SQL: {create_sql}, Error: {e}")


def copy_data_to_table(sfcon, phys_table, stage_name, file_format):
    copy_sql = f"COPY INTO {phys_table} from @~/{stage_name}/{phys_table} {file_format} on_error='continue';"
    try:
        with sfcon.cursor() as cur:
            cur.execute(copy_sql)
            logging.debug(f"Executed SQL: {copy_sql}")
    except Exception as e:
        logging.error(f"Error executing SQL: {copy_sql}, Error: {e}")


def upload_file_to_snowflake(args, sfcon, df, file_path, table_name, year, slicer):
    """
    Uploads a file to Snowflake, creating necessary stages and tables as required.
    Handles the process in a sequence of creating a stage, uploading the file,
    creating a table, and finally copying data into the table.

    Args:
        args: Command-line arguments containing output directory and other flags.
        sfcon: Snowflake connection object.
        df: Pandas DataFrame containing the data to be uploaded.
        file_path: Path of the CSV file to be uploaded.
        table_name: Name of the table to be created/used in Snowflake.
        year: Year segment for the data processing, if applicable.
        slicer: Slicer segment for the data processing, if applicable.

    Returns:
        None

    Raises:
        SnowflakeError: If any operation in Snowflake fails.
    """
    # Function implementation...

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    phys_table_parts = [table_name.replace('.', '_')]
    stg = args.output if args.output else 'oracle_stage'

    # Append year and slicer to the physical table name if they exist
    if year:
        phys_table_parts.append(str(year))
    if slicer:
        phys_table_parts.append(slicer)
    phys_table = "_".join(phys_table_parts)

    print(f"\t{now} ==> Processing {phys_table} to Snowflake account: PLAYGROUND.RAC_ACCNT")
    logging.info(f"\t{now} ==> Processing {phys_table} to Snowflake account: PLAYGROUND.RAC_ACCNT")

    file_format = "(type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"
    create_stage_in_snowflake(sfcon, stg, file_format)
    upload_file_to_stage(sfcon, file_path, stg)
    create_table_from_stage(sfcon, df, phys_table, stg)
    copy_data_to_table(sfcon, phys_table, stg, file_format)

    print(f"\t... Completed processing for {phys_table}.")
    logging.info(f"\t... Completed processing for {phys_table}.")


def read_tables_from_csv(file_path):
    print(">>> READING TABLES FROM CSV")
    print(f"=== Reading tables from csv: {file_path} ")
    tables = []

    with open(file_path, 'r') as f:
        reader = csv.reader(f, quoting=csv.QUOTE_MINIMAL )
        next(reader, None)  # Skip the header row
        for row in reader:
            if not row or row[0].startswith('#'): 
                continue
            else:
                schema_table = row[0]
                filter_using = row[1] if len(row) > 1 else None
                date_column = row[2] if len(row) > 2 else None
                slicer_column = row[3] if len(row) > 3 else None
                granularity = row[4] if len(row) > 4 else 'Y'
                tables.append({"schema_table": schema_table, "filter_using": filter_using, "date_column": date_column, "slicer_column": slicer_column, "granularity": granularity})

    print(f"... found {len(tables)} tables")
    logging.info(f"... found {len(tables)} tables")
    return tables

def get_table_details(args, tables):
    table_details = []
    for table in tables:
        details = {
            'table': table['schema_table'],
            'where_clause': '',
            'output_dir': args.output,
            'sf': args.sf,
            'start_time': None,
            'end_time': None,
            'elapsed_time': None,
            'rows_processed': 0,
            'processing_errors': []
        }
        table_details.append(details)
    return table_details

def save_table_details(table_details, file_path):
    with open(file_path, 'w') as f:
        json.dump(table_details, f)

def get_rows_processed(df, csv_file):
    if not df.empty:
        return len(df)
    else:
        with open(csv_file, 'r') as f:
            return sum(1 for line in f) - 1  # Subtract 1 to exclude the header
        
def extract_and_upload(args, table_details, output_dir, con_snowflake):
    print(f"\n>>>> STARTING EXTRACT AND UPLOAD FOR TABLE: {table_details['table']}")

    schema, base_table_name = table_details['schema_table'].split(".")
    granularity = table_details['granularity']
    current_year = datetime.now().year

    # Generate time segments based on granularity
    if granularity == 'Y':
        time_segments = generate_yearly_segments(2021, current_year)  # Adjust start year as needed
    elif granularity == 'YM':
        time_segments = generate_monthly_segments(2021, current_year)  # Adjust start year as needed
    else:
        raise ValueError("Unsupported granularity type")

    for segment in time_segments:
        table_name_with_segment = f"{base_table_name}_{segment}"
        if table_details['slicer_column']:
            table_name_with_segment += "_slice"

        csv_file = f"{output_dir}/{table_name_with_segment}.csv"

        # Skip processing if file already exists and replace flag is not set
        if not args.replace and os.path.exists(csv_file):
            print(f"::: Skipping {table_name_with_segment}... already extracted")
            continue

        start_time = time.time()

        # Update table details for specific segment
        table_details_updated = table_details.copy()
        table_details_updated['table'] = table_name_with_segment
        table_details_updated['year'] = segment[:4]
        table_details_updated['month'] = segment[4:] if granularity == 'YM' else None

        process_extraction(args, table_details_updated, output_dir)

        end_time = time.time()
        table_details_updated['start_time'] = start_time
        table_details_updated['end_time'] = end_time
        table_details_updated['elapsed_time'] = end_time - start_time
        table_details_updated['rows_processed'] = get_rows_processed(csv_file)

        if con_snowflake:
            upload_file_to_snowflake(args, con_snowflake, csv_file, table_name_with_segment)

        print(f"::: Completed processing for {table_name_with_segment}")
        save_table_details(table_details_updated, f"{output_dir}/{table_name_with_segment}_details.json")
    

def process_extraction(args, table_details, output_dir):
    full_table_name = table_details['table']
    where_clause = table_details.get('filter_using', '')
    slicer_column = table_details.get('slicer_column')
    num_slices = args.p if slicer_column else 1  # Use slicing only if slicer column is provided

    try:
        con = connect_to_oracle()

        for slice in range(num_slices):
            # Constructing the file name for CSV
            csv_file_name_suffix = f"_slice{slice}" if slicer_column else ""
            csv_file_name = f"{full_table_name}{csv_file_name_suffix}.csv"
            csv_file = os.path.join(output_dir, csv_file_name)

            # Building the SQL query with slicer logic
            slicer_clause = f" AND MOD({slicer_column}, {num_slices}) = {slice}" if slicer_column else ""
            query = f"SELECT * FROM {full_table_name} WHERE {where_clause}{slicer_clause}"
            print(f"Extracting {full_table_name} for {csv_file_name} \n\tUSING: {query}")
            logging.info(f"Extracting {full_table_name} for {csv_file_name} \n\tUSING: {query}")
            
            # Execute query and save results to CSV
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                df = pd.read_sql(query, con)
                if not df.empty:
                    df.to_csv(csv_file, index=False)
                    rows_processed = get_rows_processed(df, csv_file)
                    print(f"... {rows_processed} rows written to {csv_file}")
                    logging.info(f"... {rows_processed} rows written to {csv_file}")
                    logging.debug(f"Rows processed for {csv_file_name}: {rows_processed}")

                    # Handle Snowflake upload if applicable
                    if args.sf and con_snowflake:
                        upload_file_to_snowflake(args, con_snowflake, df, csv_file, full_table_name, None, slice if slicer_column else None)
                else:
                    print(f":: NO rows generated for {full_table_name}{csv_file_name_suffix}")

    except Exception as e:
        print(f"Error in process_extraction: {e}")
        logging.error(f"Error in process_extraction: {e}")
        table_details['processing_errors'].append(str(e))

    # finally:
    #     Close the database connections if opened.


def generate_yearly_segments(start_year, end_year):
    print(f">>>> GENERATING YEARLY SEGMENTS BETWEEN {start_year} and {end_year}")
    return [ str(year) for year in range(start_year, end_year+1)]

def generate_monthly_segments(start_year, end_year):
    print(f">>>> GENERATING MONTHLY SEGMENTS BETWEEN {start_year} and {end_year}")
    return [ f"{year}{month:02}" for year in range(start_year, end_year+1) for month in range(1,13)]


def process_args():
    """
    Parses command line arguments.

    Returns:
        argparse.Namespace: An object that holds attributes with the command line argument values. 
        This object will hold the values of output directory, parallelism level, tables to extract, 
        flags for loading tables into Snowflake, pushing CSV files into Snowflake, replacing existing CSV files, 
        and the logging level.

    Possible values:
        - output: Any valid directory path. Example: '/path/to/output'
        - p: An integer between 0 and 5. Example: 3
        - tables: A comma-separated list of table names. Example: 'table1,table2,table3'
        - sf: A boolean flag. If set, tables will be loaded into Snowflake.
        - push: A boolean flag. If set, CSV files from the output directory will be pushed into Snowflake.
        - replace: A boolean flag. If set, existing CSV files will be replaced.
        - log-level: Either 'INFO' or 'DEBUG'. Example: 'INFO'
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-o','--output', help='Output directory', required=True)
    parser.add_argument('-p', default=1, help='Parallel- 0-5, number of concurrent connections', required=False)
    parser.add_argument('-t','--tables', help='Comma separated list of tables to extract', required=False)
    parser.add_argument('--sf', action='store_true', help='If set, load tables into SF')
    parser.add_argument('--push', action='store_true', help='If set, push CSV files from output directory into SF')
    parser.add_argument('--replace', action='store_true', help='If set, replace existing CSV files')
    parser.add_argument('--log-level', choices=['INFO', 'DEBUG'], default='INFO', help='Set the logging level (default: INFO)')
    args = parser.parse_args()

    return args


def main():
    # Per Thick Client requirements: https://python-oracledb.readthedocs.io/en/latest/user_guide/troubleshooting.html#dpy-4011
    # trying to this and INIT_ORACLE_CLIENT call to thick client libraries:
    # https://python-oracledb.readthedocs.io/en/latest/user_guide/initialization.html#enablingthick
    oracledb.init_oracle_client(lib_dir=r"C:\temp\instantclient_19_21")

    setup_logging(args.log_level)
    args = process_args()
    init_oracle_client()

    if args.push:
        con_snowflake = connect_to_snowflake()
        push_csv_to_snowflake(args, con_snowflake)
    else:
        tables = read_tables_from_csv(args.tables)
        table_details_list = get_table_details(args, tables)
        con_snowflake = connect_to_snowflake() if args.sf else None

        for table_details in table_details_list:
            logging.info(f"--- PROCESSING: {table_details['table']}")
            extract_and_upload(args, table_details, args.output, con_snowflake)
            save_table_details(table_details, f"{args.output}/{table_details['table']}_details.json")
            logging.info(f"--- COMPLETED: {table_details['table']}")


if __name__ == "__main__":
    main()


# python ora2sf.py -t tables.csv -o outputs 
# python ora2sf.py -t missed.csv -o outputs -p 4 --sf
# python ora2sf.py -o outputs --sf --push --log-level DEBUG
# python ora2sf.py -o outputs --sf --replace 
