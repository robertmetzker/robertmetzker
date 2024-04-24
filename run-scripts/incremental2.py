import sys, os, datetime, argparse, re, csv
from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent
lclsetup = root/f"lclsetup"
sys.path.append(str(root))
sys.path.append(str(lclsetup))

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup, inf 

# Global variables for source and target databases
global srcdb, tgtdb


def process_args():
    # etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    incdir =f"./INCREMENTAL"
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python run_incremental.py --prev prd/etl_validation/prev_dev_source --curr prd/etl_validation/dev_source",add_help=True)

    #required
    parser.add_argument('--src', required=True, help='previous (source) env/key/database/schema for comparison (from dbsetup)')
    parser.add_argument('--tgt', required=True, help='current (target) env/key/database/schema (from dbsetup)')
    
    #optional
    parser.add_argument('--debug', required=False, default=False, action='store_true', help='add this to have all output enabled for the console')
    parser.add_argument('--init', required=False, default=False, action='store_true', help='include noinit to skip adding FB_DW_LAST_SEEN to target tables')
    parser.add_argument('--all', required=False, default=False, action='store_true', help='an additional option that sets the debug count to all records')
    parser.add_argument('--output', required=False, default=incdir, help='output directory if saving xls results is desired (uses OPENPYXL)')
    parser.add_argument('--rules', required=False, help='rules file for processing merges (table_name, matchcols)')
    
    args = parser.parse_args()

    # Added to simplify copying into Snowflake
    args.snow_prefix='@~'
    args.fields = []
    
    # args.etldir = Path( args.eldir )
    args.etldir = Path( args.output.strip() ) if args.output else Path( incdir.strip() )

    # Use the same directory for the Output as the Source SQL file, if possible.
    # args.output = Path( args.output.strip() ) if args.output else Path( args.etldir.strip() )
    # print(f"Processing Args: {args}")

    # env/db/schema <- dev/bronze/rollback or dev/ROLLBACK/ROLLBACK
    args.src = args.src.strip('/').upper()
    if args.src.count('/') == 3:
        args.srcenv, args.srckey, args.src_db, args.src_schema  = args.src.split('/')
    else:
        raise Exception("Not enough arguments in SRC connection.  Need env/key/database/schema")

    args.tgt = args.tgt.strip('/').upper()
    if args.tgt.count('/') == 3:
        args.tgtenv, args.tgtkey, args.tgt_db, args.tgt_schema = args.tgt.split('/')
    else:
        raise Exception("Not enough arguments in TGT connection.  Need env/key/database/schema")

    # Set global database variables
    global srcdb, tgtdb
    srcdb = get_arg_db_info(args, 'src')
    tgtdb = get_arg_db_info(args, 'tgt')

    print( f'=== Using ETLDIR: {args.etldir}' )

    return args

def get_arg_db_info(args, db_type):
    if db_type == 'src':
        print(f"Determining SRC database type from dbsetup...: {args.src}")
        env, key  = args.srcenv.lower(), args.srckey.lower()
    else:
        print(f"Determining TGT database type from dbsetup...: {args.tgt}")
        env, key = args.tgtenv.lower(), args.tgtkey.lower()
    return dbsetup.config['env'][env][key]

def read_rules_file(args):
    # Determine the full path to the rules file
    # rules_path = Path( args.rules ) 
    rules_path = Path( args.rules ).resolve()

    if not rules_path.is_file():
        # Attempt to find the file in the current working directory if not found in etldir
        if not rules_path.is_file():
            raise Exception(f"Error: Rules file '{args.rules}' not found in '{args.etldir}' or current working directory: {os.getcwd()}.")
            return {}

    print(f"Reading rules from: {rules_path}")
    rules = {}
    try:
        with open(rules_path, 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            headers = next(reader, None)  
            headers = [h.lower() for h in headers]
            for row in reader:
                if not row or row[0].startswith('#'):
                    continue
                else:
                    full_table_name = row[headers.index('schema_table')] 
                    match_cols_str = row[headers.index('match_cols')] if 'match_cols' in headers else None

                    if '.' in full_table_name:
                        schema, table_name = full_table_name.split(".")
                    else:
                        schema, table_name = args.tgt_schema, full_table_name
                    match_cols = match_cols_str.split(",") if match_cols_str else []
                    rules[full_table_name] = {
                        'schema': schema,
                        'table': table_name,
                        'matchcols': [col.strip() for col in match_cols]
                    }
    except Exception as e:
        print(f"Failed to read rules file: {e}")

    print(f"===> Rules: {rules}")
    return rules


def process_tables(args, rules):
    # sql += "select table_catalog, table_schema, table_name, sel_cols, ignore_cols, match_cols from cols "

    processed_tables = []

    # If rules exist, loop through each specified table in the rules
    if rules:
        for full_table_name in rules.keys():
            schema = rules[full_table_name]['schema']
            table_name = rules[full_table_name]['table']
            # Assuming generate_columns is your function to process each table
            select_columns, match_columns = generate_columns(args, full_table_name, rules)
            processed_tables.append((schema, table_name, select_columns, match_columns))
    else:
        # If no rules are provided, fetch all tables from args.tgt_schema and process them
        all_tables = fetch_tables_from_schema(args)
        for table_name in all_tables:
            full_table_name = f"{args.tgt_schema}.{table_name}"
            select_columns, match_columns = generate_columns(args, full_table_name, {})
            processed_tables.append((args.tgt_schema, table_name, select_columns, match_columns))

    return processed_tables

def assign_table_flags(tables, rules):
    """
    Assigns processing flags to tables based on rules presence.
    :param tables: List of all tables.
    :param rules: Dictionary of rules per table (empty if no rules.csv).
    :return: List of tables with processing flags.
    """
    # If rules are provided, flag only tables present in rules for processing
    if rules:
        for table in tables:
            table['process'] = table['name'] in rules
    else:
        # If no rules, flag all tables for processing
        for table in tables:
            table['process'] = True
    return tables

def fetch_all_columns(args):
    """
    Fetches all columns for all tables within the specified schema.
    """
    sql = f"""with cols as (
        SELECT table_catalog, table_schema, table_name, 
            listagg( (case when column_name not like 'FB_DW%' then column_name end) , ', ') as sel_cols,
            listagg( (case when column_name like 'FB_DW%' then column_name end) , ', ') as ignore_cols
        FROM {args.tgt_db}.information_schema.columns
    WHERE table_schema = '{args.tgt_schema}'
    AND table_schema NOT IN ('PUBLIC', 'INFORMATION_SCHEMA')
    GROUP BY table_catalog, table_schema, table_name
    )
        select table_catalog, table_schema, table_name, sel_cols from cols;
    """
    results = inf.run_sql((tgtdb, sql))
    # Assuming results are in the form of [(table_name, column_name), ...]
    return results

def process_columns_with_rules(all_columns, rules):
    """
    Processes all columns against rules defined in `rules.csv`. The all_columns parameter
    is expected to be a list of tuples, with each tuple containing:
    (schema, table, concatenated_columns)
    """
    processed = {}
     # Build a list of tables to process
    if rules:
        tables_to_process = [table for db, schema, table, concatenated_columns in all_columns if table in rules]
    else:
        tables_to_process = [table for db, schema, table, concatenated_columns in all_columns]

    for db, schema, table, concatenated_columns in all_columns:
        if table not in tables_to_process:
            continue
        # Split the concatenated column string into a list of column names
        columns = concatenated_columns.split(", ")

        if table not in processed:
            processed[table] = {'select_cols': [], 'ignore_cols': [], 'match_cols': []}

        for column in columns:
            # Determine if the column is a match column based on rules
            if table in rules and column in rules[table]['matchcols']:
                processed[table]['match_cols'].append(column)
                # Also consider match columns as ignored for selection purposes
                processed[table]['ignore_cols'].append(column)
            elif column.startswith('FB_DW'):
                processed[table]['ignore_cols'].append(column)
            else:
                # Only add to select_cols if not already added to ignore_cols
                if column not in processed[table]['ignore_cols']:
                    processed[table]['select_cols'].append(column)

    return processed


def initialize_tables(args, tables):
    """
    Adds FB_DW_LAST_SEEN, FB_DW_DATE_ADDED to all tables if not present.
    """
    print(f"\n++++ Adding FB_DW_LAST_SEEN, FB_DW_DATE_ADDED to all tables @ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
    tgt_db = args.tgt_db if args.tgt_db else 'INC_DEV_SOURCE'
    tgt_schema = args.tgt_schema  # Assuming tgt_schema is always provided

    alter_sql = []
    for table_name in tables.keys():
        # sql = f'''ALTER TABLE "{tgt_db}"."{tgt_schema}"."{table_name}" ADD COLUMN IF NOT EXISTS FB_DW_LAST_SEEN TEXT, ADD COLUMN IF NOT EXISTS FB_DW_DATE_ADDED DATE;'''
        sql = f'''ALTER TABLE "{tgt_db}"."{tgt_schema}"."{table_name}" ADD COLUMN FB_DW_LAST_SEEN TEXT, FB_DW_DATE_ADDED DATE;'''
        alter_sql.append(sql)

    all_args = ('ALTER TABLE',srcdb, alter_sql)
    if len(alter_sql) > 0 :
        print(f'ALTER SQLs:\n{alter_sql}')
        if not args.debug:
            inf.run_sql_parallel( all_args )
    else:
        print(f"\t No tables required altering...")

    print(f"\n++++ DONE altering tables @ {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")



def generate_columns( args, full_table_name, rules ):
    # Placeholder function to fetch table names from a schema
    # Split full_table_name and restrict to table_schema and table_name in sql

    if args.tgt_schema:
        print( f"\tRESTRICTED to target schema: {args.tgt_schema}\n","-"*60  )

    sql = f'''with cols as (
        SELECT table_catalog, table_schema, table_name, 
            listagg( (case when column_name not like 'FB_DW%' then column_name end) , ', ') as sel_cols,
            listagg( (case when column_name like 'FB_DW%' then column_name end) , ', ') as ignore_cols  
            '' as match_cols
        FROM {args.src_db}.information_schema.columns 
        WHERE '''
    if args.tgt_schema:
        sql += f''' table_schema = {args.tgt_schema!r} '''
    else:
        sql += '''  table_schema not in ('PUBLIC','INFORMATION_SCHEMA') '''
        
    sql += f"\n\tGROUP BY table_catalog, table_schema, table_name ) " 
    sql += "select table_catalog, table_schema, table_name, sel_cols, ignore_cols, match_cols from cols "
    sql += "where not rlike (table_name, '.*(_lcl)[0-9]+')"

    schema = args.tgt_schema if args.tgt_schema else 'ALL SCHEMAS'
    print(f'::: Generating Column Lists for Target Schema: {args.tgt_schema} :::')   
    print(f'\n-----\n{sql}\n-----') 

    inf.run_sql( (srcdb, "use secondary roles all") )
    all_args = (srcdb, sql)
    results = inf.run_sql( all_args )

    print( f'\n\t===> {len(results)} table column lists generated  <===\n')
        
    return results


def generate_queries(args, processed_info):
    ''' 
    {'TABLE_CATALOG': 'PREV_DEV_SOURCE', 'TABLE_SCHEMA': 'BASE', 'TABLE_NAME': 'PRO_ALT_ID', 'SEL_COLS': 'PRO_ID, ID_VALUE, ID_TYPE', 'IGNORE_COLS': 'FB_DW_EXTRACT_TS, FB_DW_LOAD_KEY'}

merge into   public.ref           tgt
using        DEV_SOURCE.BASE.REF  src
on ( hash(tgt.REF_DGN, tgt.REF_DLM, tgt.REF_ENT_DTE, tgt.REF_IDN, tgt.REF_DSC, tgt.REF_EXP_DTE, tgt.REF_ULM, tgt.REF_EFF_DTE, tgt.REF_ENT_UID, tgt.REF_RID) =
     hash(src.REF_DGN, src.REF_DLM, src.REF_ENT_DTE, src.REF_IDN, src.REF_DSC, src.REF_EXP_DTE, src.REF_ULM, src.REF_EFF_DTE, src.REF_ENT_UID, src.REF_RID) )
when matched then update set   tgt.FB_DW_LAST_SEEN = src.FB_DW_LOAD_KEY
when not matched then insert (tgt.REF_DGN, tgt.REF_DLM, tgt.REF_ENT_DTE, tgt.REF_IDN, tgt.REF_DSC, tgt.REF_EXP_DTE, tgt.REF_ULM, tgt.REF_EFF_DTE, tgt.REF_ENT_UID, tgt.REF_RID,  
        tgt.FB_DW_LOAD_KEY, tgt.FB_DW_EXTRACT_TS,  tgt.FB_DW_LAST_SEEN )
values (src.REF_DGN, src.REF_DLM, src.REF_ENT_DTE, src.REF_IDN, src.REF_DSC, src.REF_EXP_DTE, src.REF_ULM, src.REF_EFF_DTE, src.REF_ENT_UID, src.REF_RID, 
        src.FB_DW_LOAD_KEY, src.FB_DW_EXTRACT_TS,  src.FB_DW_LOAD_KEY );

    '''
    delta_sql = []
    print(f'\n== GENERATING DELTA QUERIES for processed tables')

    # Assuming processed_info is a dictionary with table names as keys
    for table_name, table_info in processed_info.items():
        supplemental_cols = ''
        schema = args.tgt_schema
        tbl = f'"{schema}"."{table_name}"'
        include_cols = table_info['select_cols']
        match_cols = table_info['match_cols'] if table_info['match_cols'] else include_cols

        # Generating string lists for SQL query
        tgt_cols_str = ', '.join([f'tgt."{col}"' for col in include_cols])
        src_cols_str = ', '.join([f'src."{col}"' for col in include_cols])
        hash_on_src_cols = ', '.join([f'src."{col}"' for col in match_cols])
        hash_on_tgt_cols = ', '.join([f'tgt."{col}"' for col in match_cols])
        supplemental_cols = ', '.join([f'tgt."{col}"' for col in table_info['match_cols']])
        if table_info['match_cols']:
            supplemental_cols += ', '

        sql = f'''
    MERGE INTO {args.tgt_db}.{tbl} tgt
    USING (SELECT DISTINCT * FROM {args.src_db}.{tbl}) src
    ON (HASH({hash_on_src_cols}) = HASH({hash_on_tgt_cols}))
    WHEN MATCHED THEN 
        UPDATE SET {", ".join([f'tgt."{col}" = src."{col}"' for col in include_cols])}
         , tgt.FB_DW_LAST_SEEN = CURRENT_DATE()
    WHEN NOT MATCHED THEN 
        INSERT ({tgt_cols_str}, {supplemental_cols} FB_DW_LAST_SEEN, FB_DW_DATE_ADDED)
        VALUES ({src_cols_str}, {supplemental_cols.replace('tgt.','src.')} CURRENT_DATE(), CURRENT_DATE());
    '''
        delta_sql.append(sql)

    if args.debug:
        print('\n\n=== SAMPLE MERGE QUERIES:')
        if args.all:
            for sql in delta_sql:
                print('-' * 80, '\n', sql, '\n')
        else:
            for sql in delta_sql[:3]:
                print('-' * 80, '\n', sql, '\n')

    return delta_sql


def saveresults( args, results ):    
    from openpyxl import Workbook
    now = datetime.datetime.now()
    datestring = now.strftime("%Y%m%d")

    drctry = Path( args.output )/datestring if args.output else Path( args.etldir )
    drctry.mkdir( parents=True, exist_ok = True )
    
    result_file = args.tgt_schema.upper() if args.tgt_schema else 'ALL_SCHEMAS'     
    # compare = f"{args.conn1.replace('/','_')}_vs_{args.conn2.replace('/','_')}" if args.conn2 else f"{args.conn1.replace('/','_')}"    
    validation_file = f'{result_file}_incremental_counts_{datestring}.xlsx'

    print(f'\n\nSaving the output to {drctry}/{validation_file}')

    # Output the results of the audit queries to a worksheet via openpyxl
    wb = Workbook()
    ws = wb.active

    # Write Headers
    # results is a list:
    # ({'number of rows inserted': 0, 'number of rows updated': 35029476}, '\nMERGE into   INC_DEV_SOURCE.BASE.EDI_HEADER    tgt \nUSING        (select distinct * from DEV_SOURCE.BASE.EDI_HEADER  )   src\nON ( hash( tgt.lcl_SENDER, tgt.EDI_CONTACT_EXT, tgt.TRAN_TYPE, tgt.EDI_CONTACT_EMAIL, tgt.EDI_CONTACT_PHONE, tgt.RFRNCE_BATCH_NUM, tgt.TIME_CODE, tgt.PURPOSE_CODE, tgt.lcl_RECEIVER, tgt.lcl_SUBMITTER, tgt.ACTION_CODE, tgt.EDI_CONTACT_NAME, tgt.EDI_ID, tgt.EDI_CONTACT_FAX, tgt.TRANSACTION_TIME, tgt.ISA_GSA_ST_CTL, tgt.PARTNER_ID, tgt.TRANSACTION_DATE, tgt.EDI_HDR_ID ) =\n     hash( src.lcl_SENDER, src.EDI_CONTACT_EXT, src.TRAN_TYPE, src.EDI_CONTACT_EMAIL, src.EDI_CONTACT_PHONE, src.RFRNCE_BATCH_NUM, src.TIME_CODE, src.PURPOSE_CODE, src.lcl_RECEIVER, src.lcl_SUBMITTER, src.ACTION_CODE, src.EDI_CONTACT_NAME, src.EDI_ID, src.EDI_CONTACT_FAX, src.TRANSACTION_TIME, src.ISA_GSA_ST_CTL, src.PARTNER_ID, src.TRANSACTION_DATE, src.EDI_HDR_ID ) )\nWHEN MATCHED then UPDATE set   tgt.FB_DW_LAST_SEEN = src.FB_DW_LOAD_KEY\nWHEN NOT MATCHED then INSERT ( tgt.lcl_SENDER, tgt.EDI_CONTACT_EXT, tgt.TRAN_TYPE, tgt.EDI_CONTACT_EMAIL, tgt.EDI_CONTACT_PHONE, tgt.RFRNCE_BATCH_NUM, tgt.TIME_CODE, tgt.PURPOSE_CODE, tgt.lcl_RECEIVER, tgt.lcl_SUBMITTER, tgt.ACTION_CODE, tgt.EDI_CONTACT_NAME, tgt.EDI_ID, tgt.EDI_CONTACT_FAX, tgt.TRANSACTION_TIME, tgt.ISA_GSA_ST_CTL, tgt.PARTNER_ID, tgt.TRANSACTION_DATE, tgt.EDI_HDR_ID, tgt.FB_DW_LOAD_KEY, tgt.FB_DW_EXTRACT_TS, tgt.FB_DW_LAST_SEEN )\nvalues ( src.lcl_SENDER, src.EDI_CONTACT_EXT, src.TRAN_TYPE, src.EDI_CONTACT_EMAIL, src.EDI_CONTACT_PHONE, src.RFRNCE_BATCH_NUM, src.TIME_CODE, src.PURPOSE_CODE, src.lcl_RECEIVER, src.lcl_SUBMITTER, src.ACTION_CODE, src.EDI_CONTACT_NAME, src.EDI_ID, src.EDI_CONTACT_FAX, src.TRANSACTION_TIME, src.ISA_GSA_ST_CTL, src.PARTNER_ID, src.TRANSACTION_DATE, src.EDI_HDR_ID, src.FB_DW_LOAD_KEY, src.FB_DW_EXTRACT_TS,  src.FB_DW_LOAD_KEY )\n;            ')
    headers = [ 'ID','TABLE','INSERTED','UPDATED', 'ERROR'  ]
    ws.append( headers )

    # Write Data
    #for i in tqdm (range(1), desc="Saving..."):

    # for row in results[0]:
    for row in results:
        row_dict = {}
        # row_vals = row[0]
        row_vals = row
        # num_inserted = row_vals.get('number of rows inserted', 0)
        # num_updated = row_vals.get('number of rows updated', 0)
        num_inserted = row_vals.get('inserts', 0)
        num_updated = row_vals.get('updates', 0)
        # row_sql = row[1].strip('\n').split('\n')[0]
        # row_sql = row_sql.replace('MERGE into','').replace('tgt','').strip()
        row_dict['id'] = int(row_vals.get('id',0))+1  # Grab the MERGE statement (first line) from the trimmed sql statement
        row_dict['table'] = row_vals.get('table')  # Grab the MERGE statement (first line) from the trimmed sql statement
        row_dict['inserts'] = num_inserted
        row_dict['updates'] = num_updated
        if 'error' in row_vals:
            row_dict['ERROR'] = row_vals.get('error')
        else:
            row_dict['ERROR'] = ''
        output = list( row_dict.values() )
        try:
            ws.append( output )
        except:
            print( '--ERR was encountered while trying to save the row')
            print( row ) # {'id': 0, 'table': 'ROLLBACK.TT_GPD.DBO_SOP30200', 'inserts': 13289, 'updates': 1951, 'error': ''}
            print( output ) # [0, 'ROLLBACK.TT_GPD.DBO_SOP30200', 13289, 1951, '']

    print( f'>> Saved {drctry}/{validation_file}')
    wb.save(f'{drctry}/{validation_file}')


def replace_prev( all_args ):
    args, pkg = all_args
    prev_conn = inf.get_dbcon( pkg )
    cursor = prev_conn.cursor()
    sql= ''

    # Make sure that all rights are 
    cursor.execute( 'use secondary roles all' )
    cursor.execute( 'use role ACCOUNTADMIN' )

    if args.schema:
        print( '-'*20, f'\nCLONING Current SCHEMA ( {args.curr_db}.{args.schema} ) => Previous ( {args.prev_db}.{args.schema} )\n', '-'*20 )
        sql = f'''CREATE OR REPLACE SCHEMA {args.prev_db}.{args.schema} clone {args.curr_db}.{args.schema}'''
    else:
        print( '-'*20, f'CLONING Current DATABASE ( {args.curr_db} ) => Previous ( {args.prev_db} )')
        sql = f'''CREATE OR REPLACE DATABASE {args.prev_db} clone {args.curr_db}'''

    if args.debug:
        print( sql )
    else:
        try:
            print( sql )
            cursor.execute( sql )
            result = cursor.fetchall( ) 
            print(f'{result}: {sql[0:60]}...')
        except:
            err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
            print( f'### ERROR DURING CLONE ###:\n{err}')


def parse_merge_results(results):
    """
    Parses the results from executing merge queries.
    Each result item is expected to be a tuple where:
    - The first element is the SQL string.
    - The second element is a list containing a tuple with the counts of inserted and updated rows.
    
    Args:
        results: A list of tuples containing the results of executing merge queries.

    Returns:
        A list of dictionaries, each containing the table name, number of inserted rows, and number of updated rows.
    """
    output = []

    for item in results:
        if len(item) == 2:
            sql, counts = item
            others = ''  # or some other default value
        elif len(item) == 3:
            sql, counts, others = item
            print(f"Additional values: {others}")
        else:
            raise ValueError(f"Unexpected number of values returned: {item}")
    
        stripped_sql = re.sub(r'[\'"`]', '', sql)
        count_tuple = counts[0]  # Assuming the first (and only) element of the list is the count tuple
        inserts, updates = count_tuple

        # Attempt to extract the table name from the SQL string
        table_match = re.search(r"MERGE INTO\s+(\w+\.\w+\.\w+)\s+tgt", stripped_sql, re.IGNORECASE)
        table = table_match.group(1) if table_match else ''

        # Add item index to output  


        output.append({
            'id': results.index(item),
            'table': table,
            'inserts': inserts,
            'updates': updates,
            'error': others
        })

    return output



def main():
    """
    # --prev dev/bronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
    """
    print('STARTING  >>>>')
    args = process_args()
    # Use global `srcdb` and `tgtdb` set at the end of process_args directly instead of passing as arguments

    # Create a pkg to send to inf.get_dbcon to pick the env,db connection
    # prev_db = config['env'][args.prev_env][args.prev].get('database','')
    prev_db = srcdb['database']
    print(f" >> SRC: {srcdb['database']}")
    print(f" >> TGT: {tgtdb['database']}")

    rules = {}
    # We should only build out column lists for those in the rules file.  In addition, ignore hashing our match field
    rules = read_rules_file(args) if args.rules else {}

    # Fetch all columns from the schema
    all_columns = fetch_all_columns(args)

    # Process columns with rules (if any) and flag tables for processing.  Ignore SQL [0]
    processed_info = process_columns_with_rules(all_columns[1], rules)

    # Initialize tables if required
    if args.init:
        initialize_tables(args, processed_info)

    # Generate and execute merge queries
    all_merge_sql = generate_queries(args, processed_info)

    if args.debug:
        print(f'--- SAMPLE MERGE QUERIES:')
        max = 3 if len(all_merge_sql) > 3 else len(all_merge_sql)
        for i in range(max):
            print( all_merge_sql[i] )
    else:
        now = datetime.datetime.now()
        print( f"\n===> STARTING MERGE PROCESS @ {now.strftime('%Y-%m-%d %H:%M:%S')}...")

        all_args = ('MERGE QUERIES', srcdb, all_merge_sql )
        # parallel_results = inf.run_sql_parallel( all_args )
        results = inf.run_sql_parallel( all_args )
        output = parse_merge_results( results )

        now = datetime.datetime.now()
        print( f"\n<=== FINISHED MERGE PROCESS @ {now.strftime('%Y-%m-%d %H:%M:%S')}...")
        # print("\n",output )

        if args.output:
            print(f'\n### SAVING RESULTS ###')
            saveresults( args, output )
            # writecsv( args, results )

    print("DONE")

if __name__ == '__main__':
    main()

# PREV is our TGT for the MERGE, using CURR as the SRC
# python incremental2.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
# python incremental2.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug --output ./outputs/
# python incremental2.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --output ./outputs/
# python incremental2.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --output ./outputs/

# python incremental2.py --prev dev/fbronze/EDP_BRONZE_PROD_PREV --curr prd/bronze/EDP_BRONZE_PROD  --init --output ./inc_outputs/
# python incremental2.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --output ./outputs/
 
# python incremental2.py --tgt dev/dv_rb/ROLLBACK_DV_RAW_VAULT/SAP_ECC_PRD --src dev/dv_raw/DV_RAW_VAULT/SAP_ECC_PRD --rules ./sap_rules.csv --init --debug --all
# python incremental2.py --tgt dev/dv_raw/DV_RAW_VAULT/SAP_ECC_PRD --src dev/dv_rb/ROLLBACK_DV_RAW_VAULT/SAP_ECC_PRD --debug --all >> sap_merge.sql

# python incremental2.py --tgt dev/funct/ROLLBACK/TT_GPD --src dev/gpd/PLAYGROUND/TT_GPD --rules ./tt_gpd_rules.csv --init --output ./gpd_outputs/ 
