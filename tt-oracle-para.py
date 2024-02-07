import getpass, platform, csv, os, argparse, base64, glob, logging, warnings
from datetime import datetime
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor

import pandas as pd             # pip install pandas
import oracledb                 # pip install python-oracledb
import snowflake.connector      # pip install snowflake-connector-python


def setup_logging():
    logging.basicConfig(
        filemname = f'logs/run_extract_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
        level = logging.INFO,
        format="%(processName)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

def decode_password(encoded_password):
    return base64.b64decode(encoded_password).decode()

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
    oracledb.init_oracle_client(lib_dir=d)


def connect_to_oracle():
    user = "FIVETRAN"
    pw = 'b3VyODAyMGRlYWw='.encode("ascii") #moen
    dsn = "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=dmaslorcl01)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=e21bse.masl10devsrv1)))"
    pw = decode_password(pw)
    # jdbc:oracle:thin:@//phxdevdb01.fbin.oci:1521/DEV

    con = oracledb.connect(user=user, password=pw, dsn=dsn)
    print(f"\t- Connected to Oracle: {dsn}")
    logging.info(f"\t- Connected to Oracle: {dsn}")
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


def extract_table_ddl( schema, table_name ):
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
    con = connect_to_oracle()
    ddl_sql = f"SELECT DBMS_METADATA.GET_DDL('TABLE', '{table_name}','{schema}') FROM DUAL"
    print(f"\t<< GENERATING DDL for {schema}.{table_name}:{ddl_sql}")
    logging.info(f"\t<< GENERATING DDL for {schema}.{table_name}:{ddl_sql}")

    cursor = con.cursor()
    cursor.execute( ddl_sql )
    ddl = cursor.fetchone()[0]
    cursor.close()
    return str(ddl)

def write_ddl_to_file( ddl, table_schema, ddl_folder ):
    # Writes the provided DDL to a file in the specified output folder.
    # ddl: The DDL string to write to file.
    # table_schema: The schema name for the table.
    # ddl_folder: The folder to write the DDL file to.
    if ddl is not None:
        fname = f"{table_schema.replace('.','_')}.sql"
        os.makedirs(ddl_folder, exist_ok=True)
        ddl_output = os.path.join(ddl_folder, fname)


        with open( ddl_output, 'w' ) as file:
            file.write( ddl )
        
        print(f"\t>> WROTE {table_schema} DDL to file: {ddl_output}")
        logging.info(f"\t>> WROTE {table_schema} DDL to file: {ddl_output}")
    else:
        print(f"\t-- No DDL for {table_schema}")
        logging.info(f"\t-- No DDL for {table_schema}")

def get_column_data_types(args, schema, table_name):
    """Gets the data types for columns in the given Oracle table."""
    print(f"\n\t>> Getting Datatypes for {schema}.{table_name}")
    logging.info(f"\n\t>> Getting Datatypes for {schema}.{table_name}")

    con = connect_to_oracle()
    cursor = con.cursor()
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{table_name.upper()}'
    AND OWNER = '{schema.upper()}'
    ORDER BY COLUMN_ID
    """
    cursor.execute(query)
    col_data_types = {f"{schema}.{table_name}": {row[0]: row[1] for row in cursor.fetchall()}}
    cursor.close()
    con.close()

    if args.debug:
        print(f"\t>> {col_data_types}")
        logging.info(f"\t>> {col_data_types}")

    return col_data_types


def generate_table_create_sql(schema, table_name):
    """Generates a CREATE TABLE statement for the given Oracle table."""
    
    print(f"\n\t>> Generating CREATE TABLE for {schema}.{table_name}")
    logging.info(f"\n\t>> Generating CREATE TABLE for {schema}.{table_name}")

    con = connect_to_oracle()
    cursor = con.cursor()
    query = f"""
    SELECT COLUMN_NAME, DATA_TYPE, DATA_PRECISION, DATA_SCALE, COLUMN_ID
    FROM ALL_TAB_COLUMNS
    WHERE TABLE_NAME = '{table_name.upper()}' 
    AND OWNER = '{schema.upper()}'
    ORDER BY COLUMN_ID
    """
    
    cursor.execute(query)
    rows = cursor.fetchall()
    
    columns = []
    for row in rows:
        column_name = row[0]
        data_type = row[1]
        precision = row[2]
        scale = row[3]
        
        if data_type == 'DATE':
            data_type = 'TIMESTAMP'
        
        if precision and scale:
            data_type += f"({precision},{scale})"
            
        columns.append(f"{column_name} {data_type}")
        
    column_defs = ",\n".join(columns)
    
    create_stmt = f"""CREATE TABLE {schema}.{table_name} (
{column_defs}
);"""
    
    cursor.close()
    con.close()
    
    return create_stmt


def generate_select_query(schema, table_name, columns_data_types):
    print(f"\t>> Generating SELECT SQL for {schema}.{table_name}")
    logging.info(f"\t>> Generating SELECT SQL for {schema}.{table_name}")

    # Define how different data types should be formatted
    type_formatting = {
        "DATE": {"pre": "to_char(", "post": ", 'YYYY-MM-DD HH24:MI:SS') as "},
        "TIMESTAMP": {"pre": "to_char(", "post": ", 'YYYY-MM-DD HH24:MI:SS') as "},
    }

    select_clauses = []
    for column, data_type in columns_data_types.items():
        if data_type in type_formatting:
            # Wrap the column in the specified formatting
            pre = type_formatting[data_type]["pre"]
            post = type_formatting[data_type]["post"]
            select_clauses.append(f"{pre}\"{column}\"{post}{column}")
        else:
            # Add the column without any formatting
            select_clauses.append(f"\"{column}\"")

    select_statement = ", ".join(select_clauses)
    query = f"SELECT {select_statement} FROM \"{schema}\".\"{table_name}\""
    return query


def push_csv_to_snowflake(args):
    print(f">> PUSHING CSV FILES TO SNOWFLAKE")
    logging.info(f">> PUSHING CSV FILES TO SNOWFLAKE")
    for root, dirs, files in os.walk(args.output):
        for file in files:
            if file.endswith('.csv'):
                file_path = os.path.join(root, file)
                table_name = os.path.splitext(file)[0]
                print(f"\n... Trying to PUSH {file_path} to {table_name} ...")

                # Open the file and read just the first line to get the headers
                with open(file_path, 'r') as f:
                    headers_line = next(f)  # Read the first line directly
                headers = headers_line.strip().split('~')  # Split headers based on the tilde (~) delimiter
                
                upload_file_to_snowflake(args, headers, file_path, table_name, None, None, None)



def upload_file_to_snowflake(args, headers, file_path, table_name, year, slicer, columns_data_types):
    con = connect_to_snowflake()
    tgt_schema = 'E21_RAW'
    print(f"  >>> starting UPLOAD_FILE_TO_SNOWFLAKE for {table_name}")
    logging.info(f"  >>> starting UPLOAD_FILE_TO_SNOWFLAKE for {table_name}")

    file_format = "FILE_FORMAT = (type = csv field_delimiter = '~' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    phys_table_parts = [table_name.replace('.', '_')]

    if '.' in table_name:
        table_schema, real_table_name = table_name.split('.')
    else:
        table_schema = ''
        real_table_name = table_name

    stg = args.output if args.output else 'oracle_stage'
    stg = stg.replace('\\', '').replace('/', '')

    if year:
        phys_table_parts.append(str(year))
    if slicer:
        phys_table_parts.append(slicer)
    phys_table = "_".join(phys_table_parts)
    print(f"\t\t{now} ==> Uploading {phys_table} to SF account: PLAYGROUND.{tgt_schema}")
    logging.info(f"\t\t{now} ==> Uploading {phys_table} to SF account: PLAYGROUND.{tgt_schema}")
    
    with con.cursor() as cur:
        cur.execute(f"USE SCHEMA {tgt_schema}")
        cur.execute("USE WAREHOUSE PLAYGROUND_WH")
        create_stg = f"CREATE STAGE IF NOT EXISTS {stg} {file_format};"
        cur.execute(create_stg); print(create_stg); logging.info(create_stg)

        put_file = f"PUT file://{file_path} @~/{stg} auto_compress=true;"
        cur.execute(put_file)
        print(f"\t\t{put_file}")
        logging.info(f"\t\t{put_file}")

        oracle_to_snowflake_type_mapping = {
            'NUMBER': 'NUMBER(38,10)',
            'VARCHAR2': 'TEXT',
            'DATE': 'TIMESTAMP_NTZ',
            'SDO_GEOMETRY': 'TEXT',
            'MDS': 'TEXT',           # not sure how MDS.SDO_GEOMETRY comes across 
            # Add more mappings as necessary
        }

        if columns_data_types:
            # If column data types are provided, map them to Snowflake data types
            columns = ', '.join([
                f"{col.replace('#', '_')} {oracle_to_snowflake_type_mapping.get(columns_data_types[col], 'TEXT')}"
                for col in headers
            ])
        else:
            # Directly use headers to construct the columns string, assuming TEXT data type for all
            columns = ', '.join([f"{col.replace('#', '_')} TEXT" for col in headers])

        if args.debug:
            print(f"\n\n\t\t*** COLS: {columns} \n\n")
        
        create_sql = f"CREATE OR REPLACE TABLE {phys_table} ({columns});"
        if args.debug:
            print(f"\t\t--> {create_sql}...")  # No need to assign create_sql again
        else:
            print(f"\t\t--> {create_sql[:120]}...")  # No need to assign create_sql again

        logging.info(f"\t--> {create_sql}")
        cur.execute(create_sql)

        sql = f"""COPY INTO PLAYGROUND.{tgt_schema}.{phys_table} from @~/{stg}/{real_table_name} {file_format} on_error='continue'; """
        if args.debug:
            print(f'\t\t--> {sql}...')
        else:
            print(f'\t\t--> {sql[:120]}...')
        logging.info(f'\t\t--> {sql}')
        cur.execute(sql)
    
    print(f"... copied into {phys_table}: {sql[:120]}...")
    logging.info(f"... copied into {phys_table}")


def read_tables_from_csv(file_path):
    print("<<< READING TABLES FROM CSV")
    logging.info("<<< READING TABLES FROM CSV")
    print(f"<-- Reading tables from csv: {file_path} ")
    logging.info(f"<-- Reading tables from csv: {file_path} ")
    tables = []

    with open(file_path, 'r') as f:
        reader = csv.reader(f, quoting=csv.QUOTE_MINIMAL )
        headers = next(reader, None)  
        headers = [h.lower() for h in headers]
        for row in reader:
            if not row or row[0].startswith('#'):
                continue
            else:
                schema_table = row[headers.index('schema_table')] 
                if len(row) > 1:
                    filter_using = row[headers.index('filter_using')] if 'filter_using' in headers else None
                    date_column = row[headers.index('date_column')] if 'date_column' in headers else None
                    slicer_column = row[headers.index('slicer_column')] if 'slicer_column' in headers else None
                    granularity = row[headers.index('granularity')] if 'granularity' in headers else 'Y'
                    tables.append({"schema_table": schema_table, "filter_using": filter_using, "date_column": date_column, "slicer_column": slicer_column, "granularity": granularity})
                else:
                    tables.append({"schema_table": schema_table, "filter_using": None, "date_column": None, "slicer_column": None, "granularity": None})

    print(f"... found {len(tables)} tables")
    logging.info(f"... found {len(tables)} tables")
    return tables


def extract_and_upload(args, table_details, output_dir, con_snowflake, table_column_data_types):
    print(f"\n>> STARTING EXTRACT AND UPLOAD:\n   {table_details}")
    logging.info(f"\n>> STARTING EXTRACT AND UPLOAD:\n   {table_details}")

    schema, table_name = table_details['schema_table'].split(".")
    columns_data_types = table_column_data_types  # Use the passed column data types
    select_query = generate_select_query(schema, table_name, columns_data_types)
    combination, where_clause = '', ''

    date_column = table_details['date_column']
    slicer_column = table_details['slicer_column']
    slicer_segment = 1 if slicer_column else None
    granularity = table_details['granularity']
    filter_using = table_details['filter_using']

    if filter_using:
        base_clause = f""" where {filter_using} """
        filter_year = ''.join(c for c in filter_using if c.isdigit())
        where_clause = base_clause
        print(f"\n\n\t==> Queueing FILTERED job for {table_name} > {where_clause} ")
        logging.info(f"\n\n\t==> Queueing FILTERED job for {table_name} > {where_clause} ")
        query_sql = f"{select_query} {where_clause}"
        process_extraction(args, schema, table_name, query_sql, filter_year, output_dir, table_details['schema_table'], columns_data_types)

    else:
        current_year = datetime.now().year
        current_month = datetime.now().month

        def build_where_clause(base_clause, slicer_segment):
            slicer_clause = ''
            # slicer_clause = f" AND MOD({slicer_column},4) = {slicer_segment}" if slicer_column else ""
            return f"{base_clause}{slicer_clause}"

        if date_column:
            combinations = generate_monthly_segments(2023, current_year) if granularity =='YM' else generate_yearly_segments(2021, current_year) 

            for combination in combinations:
                if granularity == 'YM'  and int(combination) > int(f"{current_year}{current_month:02}"):
                    continue  #ignore future months
                base_clause = f""" WHERE {f" {filter_using} AND " if filter_using else ' ' }"""
                base_clause += f" TO_CHAR({date_column}, 'YYYY{'' if granularity=='Y' else 'MM'}') = '{combination}' "

                slicer_segment = ''
                where_clause = build_where_clause(base_clause, slicer_segment)
                print(f"\n\n\t==> Queueing SLICED job for {table_name} > {combination} > {where_clause} ")
                logging.info(f"\n\n\t==> Queueing SLICED job for {table_name} > {combination} > {where_clause} ")
        else:
            combination = ''
            #     process_extraction( args, schema, table_name, '', '', output_dir, table_details['schema_table'] )

        query_sql = f"{select_query} {where_clause}"
        process_extraction(args, schema, table_name, query_sql, combination, output_dir, table_details['schema_table'], columns_data_types)

def process_extraction(args, schema, table_name, query_sql, filter, output_dir, full_table_name, columns_data_types):
    start_time = datetime.now()
    print(f" <<< PROCESS_EXTRACTION for {table_name} @{start_time}")
    logging.info(f" <<< PROCESS_EXTRACTION for {table_name} @{start_time}")

    total_row_count = 0  # Initialize row count

    try:
        con = connect_to_oracle()
        cur = con.cursor()
        cur.execute(query_sql)

        csv_file_name = f"{table_name}_{filter}.csv" if filter else f"{table_name}.csv"
        target_dir = os.path.join( output_dir, schema )
        os.makedirs(target_dir, exist_ok=True)
        csv_file_path = os.path.join(target_dir, csv_file_name)

        batch_size = 1000  # Adjust batch size as needed
        first_batch = True
 
        while True:
            records = cur.fetchmany(batch_size)
            if not records:
                break

            # Convert records to a DataFrame
            df = pd.DataFrame(records, columns=[desc[0] for desc in cur.description])
            total_row_count += len(df)  # Update row count
            
            # Write DataFrame to CSV file
            mode = 'a' if os.path.exists(csv_file_path) else 'w'
            header = not os.path.exists(csv_file_path)
            df.to_csv(csv_file_path, mode=mode, header=header, index=False, sep='~', doublequote=True, escapechar='\\')

            if first_batch:
                headers = df.columns.tolist()  # Capture headers from the first batch
                first_batch = False

        cur.close()

        if args.sf and os.path.exists(csv_file_path) and not first_batch:  # Ensure there was at least one batch
            con_snowflake = connect_to_snowflake()
            # Now call upload function with headers
            upload_file_to_snowflake(args, headers, csv_file_path, full_table_name, None, None, columns_data_types)


    except Exception as e:
        print(f"Error in process_extraction: {e}")
        logging.error(f"Error in process_extraction: {e}")
        return

    end_time = datetime.now()
    elapsed_time = end_time - start_time
    print(f"::: Completed extraction of {table_name} in {elapsed_time}: from {start_time} - {end_time}")
    logging.info(f"::: Completed extraction of {table_name} in {elapsed_time}: from {start_time} - {end_time}")

    # Finally, close the Oracle connection
    con.close()



def generate_yearly_segments(start_year, end_year):
    print(f"=== GENERATING YEARLY SEGMENTS BETWEEN {start_year} and {end_year}")
    logging.info(f"=== GENERATING YEARLY SEGMENTS BETWEEN {start_year} and {end_year}")
    return [ str(year) for year in range(start_year, end_year+1)]

def generate_monthly_segments(start_year, end_year):
    print(f"==== GENERATING MONTHLY SEGMENTS BETWEEN {start_year} and {end_year}")
    logging.info(f"==== GENERATING MONTHLY SEGMENTS BETWEEN {start_year} and {end_year}")
    return [ f"{year}{month:02}" for year in range(start_year, end_year+1) for month in range(1,13)]


def process_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-o','--output', help='Output directory', required=True)
    parser.add_argument('-p', help='Parallel- 0-5, number of concurrent connections', required=False)
    parser.add_argument('-t','--tables', help='Comma separated list of tables to extract', required=False)
    parser.add_argument('--sf', action='store_true', help='If set, load tables into SF')
    parser.add_argument('--ddl', action='store_true', help='If set, generate table DDL from oracle into ddl folder')
    parser.add_argument('--ddl_only', action='store_true', help='If true, only extract the DDL and do nothing else')
    parser.add_argument('--debug', action='store_true', help='If set, display additional data to help with troubleshooting')
    parser.add_argument('--push', action='store_true', help='If set, push CSV files from output directory into SF')
    args = parser.parse_args()

    args.p = 1 if not args.p else int( args.p )

    return args

def main():
    print(":: STARTING MAIN")
    logging.info(":: STARTING MAIN")
    args = process_args()
    ddl_folder = os.path.join(args.output, 'ddl')
    if not os.path.exists(ddl_folder):
        os.makedirs(ddl_folder)
    init_oracle_client()

    if args.push:
        push_csv_to_snowflake(args)
    else:
        # Step 1: Read Tables from CSV
        tables = read_tables_from_csv(args.tables)

        # Initialize dictionary to store column data types for all tables
        all_table_data_types = {}

        # Connect to Snowflake outside the loop to use the same connection for all uploads
        con_snowflake = connect_to_snowflake() if args.sf else None

        # Step 2 & 3: Loop through each table for DDL generation and fetching column data types
        for table_details in tables:
            schema_table = table_details['schema_table']
            schema, table_name = schema_table.split(".")
            full_table_name = f"{schema}.{table_name}"

            logging.info(f"--- PROCESSING: {schema_table}")

            # Generate DDL if requested
            # TODO: Try to get the DDL and if an exception, use a new function to create the DDL instead
            if args.ddl or args.ddl_only:
                try:
                    ddl = extract_table_ddl(schema, table_name)
                except Exception as e:
                    print(f"!!! Error extracting DDL for {full_table_name}: {e}")
                    print(f"   -Attempting to generate CREATE SQL for {full_table_name}")
                    ddl = generate_table_create_sql(schema, table_name) 
                write_ddl_to_file(ddl, full_table_name, ddl_folder)


            # Fetch and store column data types, but only if not already captured
            if full_table_name not in all_table_data_types:
                all_table_data_types[full_table_name] = get_column_data_types(args, schema, table_name)

        if not args.ddl_only:
            # Step 4: Extract data and upload for each table
            for table_details in tables:
                schema_table = table_details['schema_table']
                schema, table_name = schema_table.split(".")
                full_table_name = f"{schema}.{table_name}"

                # Ensure column data types for the table are already fetched
                if full_table_name in all_table_data_types:
                    table_column_data_types = all_table_data_types[full_table_name]
                    extract_and_upload(args, table_details, args.output, con_snowflake, table_column_data_types[full_table_name])
                else:
                    logging.error(f"Column data types for {full_table_name} were not fetched.")
        logging.info(f"--- COMPLETED: {table_details}")

if __name__ == "__main__":
    main()


"""
hq8627osx:DEVOPS_INF>intel  
hq8627osx:DEVOPS_INF>arch  
i386
hq8627osx:DEVOPS_INF>pyenv activate ora_envx86                                                                                                                       main

(ora_envx86) hq8627osx:DEVOPS_INF>python test-ora.py  
"""

# python tt-oracle-para.py -t TT.csv -o TT --sf --ddl
# python tt-oracle-para.py -t TT2.csv -o TT --sf --ddl