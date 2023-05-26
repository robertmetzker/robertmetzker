from itertools import count
import sys, os, argparse, re, csv, traceback
from pathlib import Path

#other libraries
from datetime import datetime

'''
This is designed to compare the results of the PREV_DEV_SOURCE database (one schema at a time)
against DEV_SOURCE.  First look at the information schema to determine the schemas, tables, columns to compare
using a MINUS query to only get new rows.  This process should ignore any of the internal BWC_ tagged columns.
Append the new rows into the INC_DEV_SOURCE schema.

--Sugessted to use prd/etl_validation as a connection to ensure validation comparison
'''


def set_libpath():
    r'''
    Set path import to be relative to the location of the dir the prog is run from
     C:\Users\nielsenjf\bwcroot\bwcenv\bwcrun\run_createviews.py
    becomes:  C:\Users\nielsenjf\bwcroot\
    '''
    import sys,os
    from pathlib import Path
    prog_path=Path(os.path.abspath(__file__))
    root=prog_path.parent.parent.parent
    pyversion=f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('using path',root,pylibpath)

set_libpath()
from bwcsetup import dbsetup
from bwcenv.bwclib import inf,dblib


def process_args():
    # etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    etldir =f"c:/TEMP/INCREMENTAL"
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python run_incremental.py --prev prd/etl_validation/prev_dev_source --curr prd/etl_validation/dev_source",add_help=True)

    #required
    parser.add_argument('--curr', required=True, help='current env/key/database (from dbsetup)')
    
    #optional
    parser.add_argument('--debug', required=False, default=False, action='store_true', help='add this to have all output enabled for the console')
    parser.add_argument('--eldir', required=False, default=etldir,help='default directory for all extracts')
    parser.add_argument('--noinit', required=False, default=False, action='store_true', help='include noinit to skip adding BWC_DW_LAST_SEEN to target tables')
    parser.add_argument('--output', required=False, default=etldir,help='output directory if saving xls results is desired (uses OPENPYXL)')
    parser.add_argument('-p','--parallel', required=False, default=1, help='number of instances to spool up.')
    parser.add_argument('--prev', required=False, help='previous env/key/database for comparison (from dbsetup)')
    parser.add_argument('--schema', required=False, help='schema name to limit incremental comparison')
    parser.add_argument('--silent', required=False, default=False, action='store_true', help='include if you want to silence validation query log file generation')
    
    args = parser.parse_args()

    # Added to simplify copying into Snowflake
    args.snow_prefix='@~'
    args.fields = []
    
    # args.parallel = 20
    args.parallel = int( args.parallel )

    # args.etldir = Path( args.eldir )
    args.etldir = Path( args.eldir.strip() ) if args.eldir else Path( etldir.strip() )

    # Use the same directory for the Output as the Source SQL file, if possible.
    # args.output = Path( args.output.strip() ) if args.output else Path( args.etldir.strip() )
    
    args.prev=args.prev.strip('/')
    if args.prev.count('/') == 2:
        args.prev_env, args.prev, args.prev_db = args.prev.split('/')
    else:
        Exception("Not enough arguments in PREV connection.  Need env/key/db")

    if args.curr:
        args.curr=args.curr.strip('/')
        if args.curr.count('/') == 2:
            args.curr_env, args.curr, args.curr_db = args.curr.split('/')
        else:
            Exception("Not enough arguments in CURR connection.  Need env/key/db")
            
    if not args.silent:
        args.logdir = args.etldir/'logs'
        args.log = inf.setup_log(args.logdir, app = 'parent')
        args.log.info( f'processing in {args.eldir}' )
        print( f'\t=== Using ETLDIR: {etldir}' )
        print( f'--Processing in: {args.eldir}' )

    return args


def get_dbcon(args, env, db):
    '''
    Returns a database connection object.  contained in object:
    {'server': 'XDW18VRPD01.bwcad.ad.bwc.state.oh.us', 'db': 'RPD1', 'login': 'xxxx', 'passwd': 'zzzzzz', 'type': 'vertica'}
    '''

    tgtdb = dbsetup.Envs[env][db]
    con = dblib.DB(tgtdb, log='', port = tgtdb.get('port',''))
    return con


def generate_columns(args):
    '''
    Generate the SQL by looping through the available numeric columns, ignoring certain column_name endings and selecting metrics.
    e.g.

        with cols as (
        select table_catalog, table_schema, table_name, 
            listagg( (case when column_name not like 'BWC%' then column_name end) , ', ') as sel_cols,
            listagg( (case when column_name like 'BWC%' then column_name end) , ', ') as ignore_cols                   
        from PREV_DEV_SOURCE.information_schema.columns
        where table_schema not in ('PUBLIC','INFORMATION_SCHEMA')
        group by table_catalog, table_schema, table_name
    )
        select table_catalog, table_schema, table_name, sel_cols from cols;
            
    '''
    sql = ''
    schema = args.schema 

    prev_conn = get_dbcon(args, args.prev_env, args.prev )

    if args.schema:
        print( f'----\n RESTRICTED to schema: {args.schema}\n----\n' )

    prev_conn.exe( 'use secondary roles all' ) 

    dbtype = prev_conn.dbtype.lower() 
    if dbtype =='snowflake':
        sql = f'''with cols as (
            SELECT table_catalog, table_schema, table_name, 
                listagg( (case when column_name not like 'BWC_DW%' then column_name end) , ', ') as sel_cols,
                listagg( (case when column_name like 'BWC_DW%' then column_name end) , ', ') as ignore_cols  
            FROM {args.prev_db}.information_schema.columns WHERE '''
        if args.schema:
            sql += f''' table_schema = {args.schema!r} '''
        else:
            sql += '''  table_schema not in ('PUBLIC','INFORMATION_SCHEMA') '''
            
        sql += f" group by table_catalog, table_schema, table_name ) " 
        sql += "select table_catalog, table_schema, table_name, sel_cols, ignore_cols from cols "
        sql += "where not rlike (table_name, '.*(_BWC)[0-9]+')"

    all_args=( args, sql )
    schema = args.schema if args.schema else 'ALL SCHEMAS'
    print(f'::: Generating Column Lists for Schema: {schema}:::')   
    print(f'\n-----\n{sql}\n-----') 
    # results = run_sql(all_args)
    results =  prev_conn.fetchall( sql ) 
    
    print( f'===> {len(list(results))} table column lists generated  <===\n')
        
    return results


def generate_queries(args, inc_tbls):
    ''' 
    {'TABLE_CATALOG': 'PREV_DEV_SOURCE', 'TABLE_SCHEMA': 'BASE', 'TABLE_NAME': 'PRO_ALT_ID', 'SEL_COLS': 'PRO_ID, ID_VALUE, ID_TYPE', 'IGNORE_COLS': 'BWC_DW_EXTRACT_TS, BWC_DW_LOAD_KEY'}

merge into   public.ref           tgt
using        DEV_SOURCE.BASE.REF  src
on ( hash(tgt.REF_DGN, tgt.REF_DLM, tgt.REF_ENT_DTE, tgt.REF_IDN, tgt.REF_DSC, tgt.REF_EXP_DTE, tgt.REF_ULM, tgt.REF_EFF_DTE, tgt.REF_ENT_UID, tgt.REF_RID) =
     hash(src.REF_DGN, src.REF_DLM, src.REF_ENT_DTE, src.REF_IDN, src.REF_DSC, src.REF_EXP_DTE, src.REF_ULM, src.REF_EFF_DTE, src.REF_ENT_UID, src.REF_RID) )
when matched then update set   tgt.BWC_DW_LAST_SEEN = src.BWC_DW_LOAD_KEY
when not matched then insert (tgt.REF_DGN, tgt.REF_DLM, tgt.REF_ENT_DTE, tgt.REF_IDN, tgt.REF_DSC, tgt.REF_EXP_DTE, tgt.REF_ULM, tgt.REF_EFF_DTE, tgt.REF_ENT_UID, tgt.REF_RID,  
        tgt.BWC_DW_LOAD_KEY, tgt.BWC_DW_EXTRACT_TS,  tgt.BWC_DW_LAST_SEEN )
values (src.REF_DGN, src.REF_DLM, src.REF_ENT_DTE, src.REF_IDN, src.REF_DSC, src.REF_EXP_DTE, src.REF_ULM, src.REF_EFF_DTE, src.REF_ENT_UID, src.REF_RID, 
        src.BWC_DW_LOAD_KEY, src.BWC_DW_EXTRACT_TS,  src.BWC_DW_LOAD_KEY );

    '''
    delta_sql = []
    print( f'\n== GENERATING DELTA QUERIES for {len(inc_tbls)} tables')
    run_dt_str = datetime.now().strftime('%Y-%m-%d')

    # Connect to Connection1 for generating the List of Table/Columns to turn in SQL statements.
    prev_conn = get_dbcon(args, args.prev_env, args.prev )
    
    if args.debug:
        args.log.info(f'\nConnection ::PREV:: ({prev_conn.dbtype.upper()}) <== Generating SQL for columns via generate_queries')


    for row in inc_tbls :
        sql, append_sql = '',''
        tbl = f"{row['TABLE_SCHEMA']}.{row['TABLE_NAME']} "
        cols = f"{row['SEL_COLS']}"
        bwc_cols = row['IGNORE_COLS']

        # Maybe not needed, but determine (for each table) the supplemental BWC column info:
        new_cols = []

        # Generate src/tgt cols
        split_cols = cols.split(', ')
        tgt_cols = [ f'tgt.{col}' for col in split_cols ]
        src_cols = [ f'src.{col}' for col in split_cols ]
        src_cols_str = ", ".join(src_cols)
        tgt_cols_str = ", ".join(tgt_cols)
        
        if bwc_cols:
            all_bwc = bwc_cols.split(', ')
            src_bwc = [f'src.{col}' for col in all_bwc ]
            tgt_bwc = [f'tgt.{col}' for col in all_bwc ]
            src_bwc_str = ", ".join(src_bwc)
            tgt_bwc_str = ", ".join(tgt_bwc)

            sql = f'''
MERGE into   INC_DEV_SOURCE.{tbl}   tgt 
USING        (select distinct * from {args.curr_db}.{tbl} )   src
ON ( hash( {tgt_cols_str} ) =
     hash( {src_cols_str} ) )
WHEN MATCHED then UPDATE set   tgt.BWC_DW_LAST_SEEN = src.BWC_DW_LOAD_KEY
WHEN NOT MATCHED then INSERT ( {tgt_cols_str}, {tgt_bwc_str}, tgt.BWC_DW_LAST_SEEN )
values ( {src_cols_str}, {src_bwc_str},  src.BWC_DW_LOAD_KEY )
;            '''

        delta_sql.append( sql )

    if args.debug:
        print('\n\n=== SAMPLE MINUS QUERIES')
        for i in range(3):
            print( '-'*80, '\n', delta_sql[i], '\n')

    return delta_sql


def geterr(note = '', clean = True):
        error_msg = traceback.format_exc()
        if clean: error_msg = error_msg.replace('\n','')
        return f'ERROR: ###### {note} {error_msg}'


def run_sql(all_args):
    args, sql, idx = all_args

    prev_conn = get_dbcon(args, args.prev_env, args.prev )

    # Make sure that all rights are 
    prev_conn.exe( 'use secondary roles all' ) 

    # Alter the session to prevent errors on Merge conflicts
    prev_conn.exe( 'alter session set ERROR_ON_NONDETERMINISTIC_MERGE = FALSE' ) 
    
    try:
        result = prev_conn.fetchone( sql ) 
        if 'successfully' in result.get('status','MERGED'):
            print( f'++ BWC_DW_LAST_SEEN added to table: {sql}')
        else:  
            sql_str = sql.split('\n')[1]  
            print(f"{idx:>3} => {result}: {sql_str}...")
    except:
        err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
        if 'already exists' in err:
            print( f'-- BWC_DW_LAST_SEEN already exists for table: {sql}')
        else:
            print( f'### ERROR ###:\n{err}')
            print( sql )

    return result, sql if result else f'ERR: {sql}'


def replace_prev(args):
    prev_conn = get_dbcon(args, args.prev_env, args.prev )
    sql= ''

    # Make sure that all rights are 
    prev_conn.exe( 'use secondary roles all' )
    prev_conn.exe( 'use role DEV_BATCH' )

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
            result = prev_conn.fetchone( sql ) 
            print(f'{result}: {sql[0:60]}...')
        except:
            err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
            print( f'### ERROR DURING CLONE ###:\n{err}')


def saveresults( args, results ):    
    from openpyxl import Workbook
    datestring = datetime.now().strftime("%Y%m%d")

    drctry = Path( args.output )/datestring if args.output else Path( args.etldir )
    drctry.mkdir( parents=True, exist_ok = True )
    
    result_file = args.schema.upper()     
    # compare = f"{args.conn1.replace('/','_')}_vs_{args.conn2.replace('/','_')}" if args.conn2 else f"{args.conn1.replace('/','_')}"    
    validation_file = f'{result_file}_incremental_counts_{datestring}.xlsx'

    if not args.silent:
        args.log.info(f'Saving the output to {drctry}/{validation_file}')
    else:
        print(f'\n\nSaving the output to {drctry}/{validation_file}')

    # Output the results of the audit queries to a worksheet via openpyxl
    wb = Workbook()
    ws = wb.active

    # Write Headers
    # results is a list:
    # ({'number of rows inserted': 0, 'number of rows updated': 35029476}, '\nMERGE into   INC_DEV_SOURCE.BASE.EDI_HEADER    tgt \nUSING        (select distinct * from DEV_SOURCE.BASE.EDI_HEADER  )   src\nON ( hash( tgt.BWC_SENDER, tgt.EDI_CONTACT_EXT, tgt.TRAN_TYPE, tgt.EDI_CONTACT_EMAIL, tgt.EDI_CONTACT_PHONE, tgt.RFRNCE_BATCH_NUM, tgt.TIME_CODE, tgt.PURPOSE_CODE, tgt.BWC_RECEIVER, tgt.BWC_SUBMITTER, tgt.ACTION_CODE, tgt.EDI_CONTACT_NAME, tgt.EDI_ID, tgt.EDI_CONTACT_FAX, tgt.TRANSACTION_TIME, tgt.ISA_GSA_ST_CTL, tgt.PARTNER_ID, tgt.TRANSACTION_DATE, tgt.EDI_HDR_ID ) =\n     hash( src.BWC_SENDER, src.EDI_CONTACT_EXT, src.TRAN_TYPE, src.EDI_CONTACT_EMAIL, src.EDI_CONTACT_PHONE, src.RFRNCE_BATCH_NUM, src.TIME_CODE, src.PURPOSE_CODE, src.BWC_RECEIVER, src.BWC_SUBMITTER, src.ACTION_CODE, src.EDI_CONTACT_NAME, src.EDI_ID, src.EDI_CONTACT_FAX, src.TRANSACTION_TIME, src.ISA_GSA_ST_CTL, src.PARTNER_ID, src.TRANSACTION_DATE, src.EDI_HDR_ID ) )\nWHEN MATCHED then UPDATE set   tgt.BWC_DW_LAST_SEEN = src.BWC_DW_LOAD_KEY\nWHEN NOT MATCHED then INSERT ( tgt.BWC_SENDER, tgt.EDI_CONTACT_EXT, tgt.TRAN_TYPE, tgt.EDI_CONTACT_EMAIL, tgt.EDI_CONTACT_PHONE, tgt.RFRNCE_BATCH_NUM, tgt.TIME_CODE, tgt.PURPOSE_CODE, tgt.BWC_RECEIVER, tgt.BWC_SUBMITTER, tgt.ACTION_CODE, tgt.EDI_CONTACT_NAME, tgt.EDI_ID, tgt.EDI_CONTACT_FAX, tgt.TRANSACTION_TIME, tgt.ISA_GSA_ST_CTL, tgt.PARTNER_ID, tgt.TRANSACTION_DATE, tgt.EDI_HDR_ID, tgt.BWC_DW_LOAD_KEY, tgt.BWC_DW_EXTRACT_TS, tgt.BWC_DW_LAST_SEEN )\nvalues ( src.BWC_SENDER, src.EDI_CONTACT_EXT, src.TRAN_TYPE, src.EDI_CONTACT_EMAIL, src.EDI_CONTACT_PHONE, src.RFRNCE_BATCH_NUM, src.TIME_CODE, src.PURPOSE_CODE, src.BWC_RECEIVER, src.BWC_SUBMITTER, src.ACTION_CODE, src.EDI_CONTACT_NAME, src.EDI_ID, src.EDI_CONTACT_FAX, src.TRANSACTION_TIME, src.ISA_GSA_ST_CTL, src.PARTNER_ID, src.TRANSACTION_DATE, src.EDI_HDR_ID, src.BWC_DW_LOAD_KEY, src.BWC_DW_EXTRACT_TS,  src.BWC_DW_LOAD_KEY )\n;            ')
    headers = [ 'SQL','INSERTED','UPDATED','ERROR' ]
    ws.append( headers )

    # Write Data
    #for i in tqdm (range(1), desc="Saving..."):
    for row in results[0]:
        row_dict = {}
        row_vals = row[0]
        num_inserted = row_vals.get('number of rows inserted', 0)
        num_updated = row_vals.get('number of rows updated', 0)
        row_sql = row[1].strip('\n').split('\n')[0]
        row_sql = row_sql.replace('MERGE into','').replace('tgt','').strip()
        row_dict['sql'] = row_sql  # Grab the MERGE statement (first line) from the trimmed sql statement
        row_dict['inserted'] = num_inserted
        row_dict['updated'] = num_updated
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


def writecsv( args, results ):
    print( '\n--- Writing CSV ---')

    # unpack the list of lists for write_csv from infrastructure to use
    csv_list = []
    for rw in results[0]:
        csv_list.append( rw )

    datestring = datetime.now().strftime("%Y%m%d")

    schema_name = args.schema.upper()
    fields_avail = csv_list[0]

    compare = fields_avail['C1']+'_vs_'+fields_avail['C2'] if 'C2' in fields_avail  else fields_avail['C1']
    comparison_file = f'{schema_name}-{compare}_comparison_{datestring}.csv'

    # Correct the SQL columns (DB1_SQL, DB2_SQL) to change ' to '' for inserting into SF
    for row in csv_list:
        row['DB1_SQL'] = row['DB1_SQL'].replace("'","''")
        if not row.get('DB2_SQL',"") == "":
            row['DB2_SQL'] = row['DB2_SQL'].replace("'","''")
    
    args.fields = list( csv_list[0].keys() )
    print(f'**** FIELDS: {args.fields}')
    # write_csv(fname,rows,raw=False,delim='\t',term='\n',prefix='',sortit=True,log=None,verify=False):
    with open( args.etldir/comparison_file, 'w', newline = '') as file2write:
        with file2write:
            csvoutput = csv.DictWriter( file2write, args.fields )
            csvoutput.writeheader()
            csvoutput.writerows( csv_list )

    # inf.write_csv( args.etldir/comparison_file, csv_list )
    print( f' --- Wrote CSV file to: {comparison_file}\n')

    return comparison_file


def main():
    print('STARTING  >>>>')
    args = process_args()
    results = []

    if not args.silent:
        logging = True
    else:
        logging = False

    tables =  generate_columns(args)
    if not tables:
        print( f'## No tables found in the Connection specified'); exit()
    else:
        print(f'--- SAMPLE Column Extract:')
        if args.debug:
            for i in range(3):
                print( '-'*80, '\n', tables[i], '\n')

    # Create ALTER table statements for all tables and batch execute them
    if not args.noinit:
        print( f"\n++++ Adding BWC_DW_LAST_SEEN to all tables...")
        alter_sql = [ f"ALTER TABLE INC_DEV_SOURCE.{tbl['TABLE_SCHEMA']}.{tbl['TABLE_NAME']} add column BWC_DW_LAST_SEEN text " for tbl in tables ]
        all_args = [ (args, sql, idx) for idx, sql in enumerate( alter_sql ) ]
        alter_results = inf.run_parallel(run_sql, args = all_args, parallel = args.parallel, log = logging)   
        print( f"\n++++ DONE altering tables\n")
    else:
        print( f"\n^^^^ SKIPPED altering tables\n")
        
 
    if args.parallel > 1:
        print(f'\n  -- Spawning {args.parallel} processes --')
    else:
        print(f'\n  -- Running Single Threaded --')

    queries =  generate_queries(args, tables)
    all_args = [ (args, sql, idx) for idx, sql in enumerate( queries ) ]

    if args.debug:
        print(f'--- SAMPLE MERGE QUERIES:')
        for i in range(3):
            print( queries[i] )
    else:
        results.append( inf.run_parallel(run_sql, args = all_args, parallel = args.parallel, log = logging) )
        # Always write the output to Snowflake, but only output XLSX if a directory is specified
        if args.output:
            print(f'### SAVING RESULTS ###')
            saveresults( args, results )

            # writecsv( args, results )


    # for tbl in results:
    #     print(tbl)

    # replace_prev(args)
        
    print(f'### DONE ###')
    # TODO-   When a schema is complete, replace the PREV_DEV_SOURCE with the DEV_SOURCE
    # using:  create or replace schema

if __name__ == '__main__':
    main() 



# cd bwcroot\bwcenv\bwcrun
# python run_incremental.py --prev prd/etl_validation/PREV_DEV_SOURCE --curr prd/etl_validation/DEV_SOURCE  --parallel 20 --debug
# python run_incremental.py --prev prd/etl_validation/PREV_DEV_SOURCE --curr prd/etl_validation/DEV_SOURCE  --schema BASE --parallel 10  --output c:\temp --noinit --debug
# python run_incremental.py --prev prd/etl_validation/PREV_DEV_SOURCE --curr prd/etl_validation/DEV_SOURCE  --schema BASE --parallel 10
# python run_incremental.py --prev prd/etl_validation/PREV_DEV_SOURCE --curr prd/etl_validation/DEV_SOURCE  --schema BWCCMN --parallel 10
# python run_incremental.py --prev prd/etl_validation/PREV_DEV_SOURCE --curr prd/etl_validation/DEV_SOURCE  --schema PCMP --parallel 10
