import sys, os, datetime, argparse, re, csv
from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent
lclsetup = root/f"lclsetup"
sys.path.append(str(root))
sys.path.append(str(lclsetup))

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup, inf 


def process_args():
    # etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    incdir =f"./INCREMENTAL"
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python run_incremental.py --prev prd/etl_validation/prev_dev_source --curr prd/etl_validation/dev_source",add_help=True)

    #required
    parser.add_argument('--src', required=True, help='Source env/key/database for incoming comparison (from dbsetup)')
    parser.add_argument('--tgt', required=True, help='Target env/key/database for receiving changed data (from dbsetup)')
    
    #optional
    parser.add_argument('--debug', required=False, default=False, action='store_true', help='add this to have all output enabled for the console')
    parser.add_argument('--init', required=False, default=False, action='store_true', help='include noinit to skip adding FB_DW_LAST_SEEN to target tables')
    parser.add_argument('--output', required=False, default=incdir,help='output directory if saving xls results is desired (uses OPENPYXL)')
    parser.add_argument('--schema', required=False, help='schema name to limit incremental comparison')
    parser.add_argument('--match-keys-csv', required=False, help='Path to CSV file containing match key configurations')

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
    args.src = args.src.strip('/')
    if args.src.count('/') == 3:
        args.srcenv, args.srckey, args.src_db, args.src_schema  = args.src.split('/')
    elif args.src.count('/') == 2:
        args.srcenv, args.srckey, args.src_db = args.src.split('/')
        args.src_schema = None
    else:
        Exception("Not enough arguments in SRC connection.  Need env/key/database")

    args.tgt = args.tgt.strip('/')
    if args.tgt.count('/') == 3:
        args.tgtenv, args.tgtkey, args.tgt_db, args.tgt_schema  = args.tgt.split('/')
    elif args.tgt.count('/') == 2:
        args.tgtenv, args.tgtkey, args.tgt_db = args.tgt.split('/')
        args.tgt_schema = None
    else:
        Exception("Not enough arguments in TGT connection.  Need env/key/database")

    print( f'=== Using ETLDIR: {args.etldir}' )

    return args



def load_match_keys_from_csv(filepath):
    match_keys = {}
    with open(filepath, mode='r', encoding='utf-8-sig') as csvfile:
        csvreader = csv.reader(csvfile)
        for row in csvreader:
            schema_table, fields = row[0], row[1]
            match_keys[schema_table] = [field.strip() for field in fields.split(',')]
    return match_keys

    
def get_arg_db_info(args, db_type):
    if db_type == 'src':
        env = args.srcenv
        key = args.srckey
    else:
        env = args.tgtenv
        key = args.tgtkey
    return dbsetup.config['env'][env][key]


def generate_columns( args, srcdb ):
    '''
    Generate the SQL by looping through the available numeric columns, ignoring certain column_name endings and selecting metrics.
    e.g.

        with cols as (
        select table_catalog, table_schema, table_name, 
            listagg( (case when column_name not like 'FB_DW%' then column_name end) , ', ') as sel_cols,
            listagg( (case when column_name like 'FB_DW%' then column_name end) , ', ') as ignore_cols                   
        from PREV_DEV_SOURCE.information_schema.columns
        where table_schema not in ('PUBLIC','INFORMATION_SCHEMA')
        group by table_catalog, table_schema, table_name
    )
        select table_catalog, table_schema, table_name, sel_cols from cols;
            
    '''
    sql = ''
    #     prev_pkg = {'config':config, 'env':args.prev_env, 'db':args.prev, 'schema':args.schema } 

    if args.schema:
        print( f"\tRESTRICTED to schema: {args.schema}\n","-"*60  )

    sql = f'''with cols as (
        SELECT table_catalog, table_schema, table_name, 
            listagg( (case when column_name not like 'FB_DW%' then column_name end) , ', ') as sel_cols,
            listagg( (case when column_name like 'FB_DW%' then column_name end) , ', ') as ignore_cols  
        FROM {args.src_db}.information_schema.columns 
        WHERE '''
    if args.schema:
        # either_or = f'%{args.schema}'
        sql += f''' table_schema = {args.schema!r} '''
    else:
        sql += '''  table_schema not in ('PUBLIC','INFORMATION_SCHEMA') '''
        
    sql += f"\n\tGROUP BY table_catalog, table_schema, table_name ) " 
    sql += "select table_catalog, table_schema, table_name, sel_cols, ignore_cols from cols "
    sql += "where not rlike (table_name, '.*(_lcl)[0-9]+')"

    schema = args.schema if args.schema else 'ALL SCHEMAS'
    print(f'::: Generating Column Lists for Schema: {schema} :::')   
    print(f'\n-----\n{sql}\n-----') 

    inf.run_sql( (srcdb, "use secondary roles all") )
    all_args = (srcdb, sql)
    results = inf.run_sql( all_args )

    print( f'\n\t===> {len(results)} table column lists generated  <===\n')
        
    return results


def generate_queries( all_args ):
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
    args, inc_tbls = all_args

    print( f'\n== GENERATING DELTA QUERIES for {len(inc_tbls)} tables')
    now = datetime.datetime.now()
    run_dt_str = now.strftime('%Y-%m-%d')
    src_db = args.src_db.strip() if args.src_db else 'INC_DEV_SOURCE' 

    # Connect to Connection1 for generating the List of Table/Columns to turn in SQL statements.
    
    if args.debug:
        print(f'\nConnection ::PREV:: (SNOWFLAKE) <== Generating SQL for columns via generate_queries')

    for rowdata in inc_tbls :
        # db, schema, table_name, include_cols, ignore_cols = rowdata
        sql, append_sql = '',''
        cols = f"{include_cols}"
        lcl_cols = f"{ignore_cols}"
        src_lcl_str, tgt_lcl_str = "",""
        # tbl = f'"{schema}"."{table_name}"'

        schema, table_name = rowdata['table_schema'], rowdata['table_name']
        full_table_name = f"{schema}.{table_name}"
        match_keys = args.match_keys.get(full_table_name, [])

        if not match_keys:
            print(f"No match keys specified for {full_table_name}, skipping.")
            continue

        # Generate hash expressions for match keys
        match_keys_expr = ', '.join([f"nvl(src.\"{field}\", 'null_value_placeholder')" for field in match_keys])
        # Maybe not needed, but determine (for each table) the supplemental lcl column info:
        new_cols = []

        # Generate src/tgt cols
        split_cols = cols.split(', ')
        tgt_cols = [ f'tgt."{col}"' for col in split_cols ]
        src_cols = [ f'src."{col}"' for col in split_cols ]
        src_cols_str = ", ".join(src_cols)
        tgt_cols_str = ", ".join(tgt_cols)
        
        if lcl_cols:
            all_lcl = lcl_cols.split(', ')
            src_lcl = [f'src."{col}"' for col in all_lcl ]
            tgt_lcl = [f'tgt."{col}"' for col in all_lcl ]
            src_lcl_str = ", ".join(src_lcl)
            tgt_lcl_str = ", ".join(tgt_lcl)

        sql = f'''
MERGE INTO {args.tgt_db}.{full_table_name} tgt 
USING (SELECT * FROM {args.src_db}.{full_table_name}) src
ON ( hash( {match_keys_expr} ) = hash({match_keys_expr.replace('src.', 'tgt.')}))
WHEN MATCHED THEN UPDATE SET  tgt.FB_DW_LAST_SEEN = CURRENT_DATE()
WHEN NOT MATCHED then 
INSERT ( {tgt_cols_str}, tgt.FB_DW_LAST_SEEN, tgt.FB_DW_DATE_ADDED )
values ( {src_cols_str}, CURRENT_DATE(), CURRENT_DATE()  )
;            '''

        delta_sql.append( sql )

    if args.debug:
        print('\n\n=== SAMPLE MINUS QUERIES')
        for i in range(3):
            print( '-'*80, '\n', delta_sql[i], '\n')

    return delta_sql


def saveresults( args, results ):    
    from openpyxl import Workbook
    now = datetime.datetime.now()
    datestring = now.strftime("%Y%m%d")

    drctry = Path( args.output )/datestring if args.output else Path( args.etldir )
    drctry.mkdir( parents=True, exist_ok = True )
    
    result_file = args.schema.upper() if args.schema else 'ALL_SCHEMAS'     
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
            row_dict['ERROR'] = row_vals
        else:
            row_dict['ERROR'] = ''
        output = list( row_dict.values() )
        try:
            ws.append( output )
        except:
            print( '--ERR was encountered while trying to save the row')

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
        print( '-'*20, f'\nCLONING Current SCHEMA ( {args.curr_db}.{args.schema} ) => Previous ( {args.src_db}.{args.schema} )\n', '-'*20 )
        sql = f'''CREATE OR REPLACE SCHEMA {args.src_db}.{args.schema} clone {args.curr_db}.{args.schema}'''
    else:
        print( '-'*20, f'CLONING Current DATABASE ( {args.curr_db} ) => Previous ( {args.src_db} )')
        sql = f'''CREATE OR REPLACE DATABASE {args.src_db} clone {args.curr_db}'''

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


def parse_merge_results ( results ):
    # ((0, '\nMERGE into   INC_DEV_SOURCE."SHOPIFY_FIBERON"."REFUND"   tgt \nUSING        (select distinct * from EDP_BRONZE_PROD."SHOPIFY_FIBERON"."REFUND" )   src\nON ( hash( tgt."ID", tgt."CREATED_AT", tgt."NOTE", tgt."ORDER_ID", tgt."TOTAL_DUTIES_SET", tgt."_FIVETRAN_SYNCED", tgt."USER_ID", tgt."RESTOCK", tgt."PROCESSED_AT" ) =\n     hash( src."ID", src."CREATED_AT", src."NOTE", src."ORDER_ID", src."TOTAL_DUTIES_SET", src."_FIVETRAN_SYNCED", src."USER_ID", src."RESTOCK", src."PROCESSED_AT" ) )\nWHEN MATCHED then UPDATE set   tgt.FB_DW_LAST_SEEN = CURRENT_DATE()\nWHEN NOT MATCHED then \nINSERT ( tgt."ID", tgt."CREATED_AT", tgt."NOTE", tgt."ORDER_ID", tgt."TOTAL_DUTIES_SET", tgt."_FIVETRAN_SYNCED", tgt."USER_ID", tgt."RESTOCK", tgt."PROCESSED_AT", tgt."FB_DW_LAST_SEEN" )\nvalues ( src."ID", src."CREATED_AT", src."NOTE", src."ORDER_ID", src."TOTAL_DUTIES_SET", src."_FIVETRAN_SYNCED", src."USER_ID", src."RESTOCK", src."PROCESSED_AT", CURRENT_DATE()  )\n;            '), [(1,100)])
    # Parse each line of the dict into:  ID, SQL, INSERTS, UPDATES
    output = []

    for item in results:
        id, sql = item[0]
        table = re.search(r"MERGE into (.*?) tgt", sql, re.IGNORECASE | re.DOTALL)
        if table:
            table = table.group(1).replace('"','').strip()
        else:
            table = ''

        inserts = item[1][0][0]
        updates = item[1][0][1]

        output.append( {'id':id, 'table':table, 'inserts':inserts, 'updates':updates })

    return output


def main():
    """
    # --src dev/bronze/INC_DEV_SOURCE --tgt prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
    """
    print('STARTING  >>>>')
    args = process_args()
    
    if args.match_keys_csv:
        args.match_keys = load_match_keys_from_csv( args.match_keys_csv )
    else:
        args.match_keys = {}

    if args.src:
        print(f"Determining SRC database type from dbsetup...")
        srcdb = get_arg_db_info(args,'src')
        print(f" >> SRC: {srcdb}")
    else:
        print(f"!!! No SRC database specified")
        srbdb = None
        
    if args.tgt:
        print(f"Determining TGT database type from dbsetup...")
        tgtdb = get_arg_db_info(args,'tgt')
        print(f" >> TGT: {tgtdb}")
    else:
        print(f"!!! No TGT database specified")
        tgtdb = None
            
    # Create a pkg to send to inf.get_dbcon to pick the env,db connection
    # src_db = config['env'][args.prev_env][args.prev].get('database','')
    src_db = srcdb['database']
    print(f" >> PREV: {src_db}")

    # Test establishing a connection
    print("\n","="*60,f"\n Generating Column Info for {src_db} ...\n","="*60)
    _run_sql, tables =  generate_columns(args, srcdb )
    if not tables:
        print( f'## No tables found in the Connection specified'); exit()
    else:
        if args.debug:
            print(f'--- SAMPLE Column Extract:')
            for i in range(3):
                print( '-'*80, '\n', tables[i], '\n')

    table_list = []
    for table in tables:
        db, schema, table_name, cols, ignores = table
        table_list.append(f'"{schema}"."{table_name}"')

    # If set to Initialize, build SQL and run_sql_parallel
    if args.init:
        now = datetime.datetime.now()
        print( f"\n++++ Adding FB_DW_LAST_SEEN, FB_DW_DATE_ADDED to all tables @ {now.strftime('%Y-%m-%d %H:%M:%S')}...")
        src_db = args.src_db if args.src_db else 'INC_DEV_SOURCE'

        # print( f"For the following tables:\n {tables}", '\n','='*80 )
        alter_sql = [ f"""ALTER TABLE "{src_db}".{tbl} add column FB_DW_LAST_SEEN text, FB_DW_DATE_ADDED date """ for tbl in table_list if ignores =='' ]
        all_args = [ (args, sql, idx) for idx, sql in enumerate( alter_sql ) ]
        all_args = ('ALTER TABLE',srcdb, alter_sql)
        if len(alter_sql) > 0 :
            print(f'ALTER SQLs:\n{alter_sql}')
            if not args.debug:
                inf.run_sql_parallel( all_args )
        else:
            print(f"\t No tables required altering...")

        now = datetime.datetime.now()
        print( f"\n++++ DONE altering tables @ {now.strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        print( f"\n^^^^ SKIPPED altering tables\n")
        
    # After making sure that target columns exist, being generating the MERGE_SQL by passing our table_list.
    all_args = (args, tables )
    queries =  generate_queries( all_args )

    all_merge_sql = [ sql for sql in enumerate( queries ) ]
    
    if args.debug:
        print(f'--- SAMPLE MERGE QUERIES:')
        for i in range(3):
            print( queries[i] )
    else:
        # for each in range(len(all_args)): 
        # result = run_merge( args, each )
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

    # Once complete, swap the previous database by clone back over with the current.
    # all_args = (args, prev_pkg )
    # replace_prev( all_args )

    print("DONE")
    # print(f"In parallel: {new_results}")

if __name__ == '__main__':
    main()

# PREV is our TGT for the MERGE, using CURR as the SRC
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug --output ./outputs/
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --output ./outputs/
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --output ./outputs/

# python incremental.py --prev dev/fbronze/EDP_BRONZE_PROD_PREV --curr prd/bronze/EDP_BRONZE_PROD  --init --output ./inc_outputs/
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --output ./outputs/
 
# python incremental.py --prev dev/funct/ROLLBACK --curr dev/gpd/PLAYGROUND  --schema TT_GPD --init --output ./gpd_outputs/
