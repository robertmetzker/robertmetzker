import sys, os, datetime, argparse, re
from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent                  # Back from Run folder to Root DEVOPS_INF
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
    parser.add_argument('--curr', required=True, help='current env/key/database (from dbsetup)')
    
    #optional
    parser.add_argument('--debug', required=False, default=False, action='store_true', help='add this to have all output enabled for the console')
    parser.add_argument('--init', required=False, default=False, action='store_true', help='include noinit to skip adding FB_DW_LAST_SEEN to target tables')
    parser.add_argument('--output', required=False, default=incdir,help='output directory if saving xls results is desired (uses OPENPYXL)')
    parser.add_argument('--prev', required=False, help='previous env/key/database for comparison (from dbsetup)')
    parser.add_argument('--schema', required=False, help='schema name to limit incremental comparison')
    
    args = parser.parse_args()

    # Added to simplify copying into Snowflake
    args.snow_prefix='@~'
    args.fields = []
    
    # args.etldir = Path( args.eldir )
    args.etldir = Path( args.output.strip() ) if args.output else Path( incdir.strip() )

    # Use the same directory for the Output as the Source SQL file, if possible.
    # args.output = Path( args.output.strip() ) if args.output else Path( args.etldir.strip() )
    
    args.prev=args.prev.strip('/')
    # env/db/schema <- dev/bronze/rollback or dev/ROLLBACK/ROLLBACK
    if args.prev.count('/') == 2:
        args.prev_env, args.prev, args.prev_db = args.prev.split('/')
    else:
        Exception("Not enough arguments in PREV connection.  Need env/key/database")

    if args.curr:
        args.curr=args.curr.strip('/')
        if args.curr.count('/') == 2:
            args.curr_env, args.curr, args.curr_db = args.curr.split('/')
        else:
            Exception("Not enough arguments in CURR connection.  Need env/key/database")
            
    print( f'=== Using ETLDIR: {args.etldir}' )

    return args


def generate_columns( args, prev_pkg ):
    '''
    Generate the SQL by looping through the available numeric columns, ignoring certain column_name endings and selecting metrics.
    e.g.

    with tbls as ( select table_catalog, table_schema, table_name from information_schema.tables 
        where table_type !='VIEW' and table_schema not in ('PUBLIC','INFORMATION_SCHEMA')),
    cols as (
        select src.table_catalog, src.table_schema, src.table_name, 
            listagg( (case when src.column_name not like 'FB_DW%' then src.column_name end) , ', ') as sel_cols,
            listagg( (case when src.column_name like 'FB_DW%' then src.column_name end) , ', ') as ignore_cols                   
        from information_schema.columns src
        join tbls on (src.table_catalog = tbls.table_catalog
                and   src.table_schema = tbls.table_schema
                and   src.table_name = tbls.table_name )
        group by src.table_catalog, src.table_schema, src.table_name
        )
    select table_catalog, table_schema, table_name, sel_cols from cols;     
    '''
    sql = ''
    #     prev_pkg = {'config':config, 'env':args.prev_env, 'db':args.prev, 'schema':args.schema } 

    if args.schema:
        print( f"\tRESTRICTED to schema: {args.schema}\n","-"*60  )

    sql = f'''
       with tbls as ( select table_catalog, table_schema, table_name from {args.prev_db}.information_schema.tables 
        where table_type !='VIEW' and table_schema not in ('PUBLIC','INFORMATION_SCHEMA')),
    cols as (
        select src.table_catalog, src.table_schema, src.table_name, 
            listagg( (case when src.column_name not like 'FB_DW%' then src.column_name end) , ', ') as sel_cols,
            listagg( (case when src.column_name like 'FB_DW%' then src.column_name end) , ', ') as ignore_cols                   
        from {args.prev_db}.information_schema.columns src
        join tbls on (src.table_catalog = tbls.table_catalog
                and   src.table_schema = tbls.table_schema
                and   src.table_name = tbls.table_name )
        WHERE 1=1 and '''
    if args.schema:
        # either_or = f'%{args.schema}'
        sql += f''' src.table_schema = {args.schema!r} '''
    else:
        sql += '''  src.table_schema not in ('PUBLIC','INFORMATION_SCHEMA') '''
    sql += " group by src.table_catalog, src.table_schema, src.table_name ) "        
    sql += "select table_catalog, table_schema, table_name, sel_cols, ignore_cols from cols "
    sql += "where not rlike (table_name, '.*(_lcl)[0-9]+')"

    schema = args.schema if args.schema else 'ALL SCHEMAS'
    print(f'::: Generating Column Lists for Schema: {schema} :::')   
    print(f'\n-----\n{sql}\n-----') 

    inf.run_sql( (prev_pkg, "use secondary roles all") )
    all_args = (prev_pkg, sql)
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
    prev_db = args.prev_db.strip() if args.prev_db else 'INC_DEV_SOURCE' 

    # Connect to Connection1 for generating the List of Table/Columns to turn in SQL statements.
    
    if args.debug:
        print(f'\nConnection ::PREV:: (SNOWFLAKE) <== Generating SQL for columns via generate_queries')


    for rowdata in inc_tbls :
        db, schema, table_name, include_cols, ignore_cols= rowdata
        sql, append_sql = '',''
        tbl = f'"{schema}"."{table_name}"'
        cols = f"{include_cols}"
        lcl_cols = f"{ignore_cols}"
        src_lcl_str, tgt_lcl_str = "",""

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
MERGE into   {prev_db}.{tbl}   tgt 
USING        (select distinct * from {args.curr_db}.{tbl} )   src
ON ( hash( {tgt_cols_str} ) =
     hash( {src_cols_str} ) )
WHEN MATCHED then UPDATE set   tgt.FB_DW_LAST_SEEN = CURRENT_DATE()
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
    # --prev dev/bronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
    """
    print('STARTING  >>>>')
    args = process_args()
    config = dbsetup.config[0]

    # Create a pkg to send to inf.get_dbcon to pick the env,db connection
    prev_db = config['env'][args.prev_env][args.prev].get('database','')
    prev_pkg = {'config':config, 'env':args.prev_env, 'db':args.prev, 'schema':args.schema, 'database':prev_db } 

    # Test establishing a connection
    print("\n","="*60,f"\n Generating Column Info for {prev_db} ...\n","="*60)
    _run_sql, tables =  generate_columns(args, prev_pkg )
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
        prev_db = args.prev_db if args.prev_db else 'INC_DEV_SOURCE'

        # print( f"For the following tables:\n {tables}", '\n','='*80 )
        alter_sql = [ f"""ALTER TABLE "{prev_db}".{tbl} add column FB_DW_LAST_SEEN text, FB_DW_DATE_ADDED date """ for tbl in table_list if ignores =='' ]
        all_args = [ (args, sql, idx) for idx, sql in enumerate( alter_sql ) ]
        all_args = ('ALTER TABLE',prev_pkg, alter_sql)
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

        all_args = ('MERGE QUERIES', prev_pkg, all_merge_sql )
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

# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug --output ../outputs/
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --output ../outputs/
# python incremental.py --prev dev/fbronze/EDP_BRONZE_PROD_PREV --curr prd/bronze/EDP_BRONZE_PROD  --init --output ../inc_outputs/
# python incremental.py --prev dev/fbronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --output ../outputs/

# python incremental.py --prev dev/fbronze/EDP_BRONZE_PROD_PREV --curr prd/bronze/EDP_BRONZE_PROD  --schema SAP_ECC_PRD --output ../inc_outputs/
# python incremental.py --prev dev/fbronze/EDP_BRONZE_PROD_PREV --curr prd/bronze/EDP_BRONZE_PROD  --output ../inc_outputs/
