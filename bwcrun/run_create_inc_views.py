from itertools import count
import sys, os, argparse, re, csv, traceback
from pathlib import Path

#other libraries
from datetime import datetime

'''
This is designed to generate INC_VIEWS database (one schema at a time)
against INC_DEV_SOURCE.  First look at the information schema to determine the schemas, tables

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
    parser = argparse.ArgumentParser(description='command line args', epilog="Example:python run_create_inc_views.py --curr prd/etl_validation/INC_DEV_SOURCE", add_help=True)

    #required
    parser.add_argument('--curr', required=True, help='current env/key/database (from dbsetup)')
    
    #optional
    parser.add_argument('--debug', required=False, default=False, action='store_true', help='add this to have all output enabled for the console')
    parser.add_argument('--eldir', required=False, default=etldir,help='default directory for all extracts')
    parser.add_argument('--init',  required=False, default=False, action='store_true', help='include init to attempt to create missing schemas')
    parser.add_argument('-p','--parallel', required=False, default=1, help='number of instances to spool up.')
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


def generate_view_sql( args, inc_tbls ):
    '''
    create or replace view  {db}.{schema}.{table_name}_VW as (
        with mx as (select max(bwc_dw_load_key) as max_load_key 
                    from INC_DEV_SOURCE.{schema}.{table_name}}
                ),
        compare as (
        select 
            case when  bwc_dw_last_seen = max_load_key then 'Y' end as CURRENT_IND
            ,case when nvl(current_ind,'X') != 'Y' then 'D' 
                when bwc_dw_last_seen >  bwc_dw_load_key  then 'U' 
                when bwc_dw_load_key = bwc_dw_last_seen then 'I'
                end as STATUS 
            ,* from INC_DEV_SOURCE.{schema}.{table_name}
            left join mx on (bwc_dw_last_seen = max_load_key)
        )
    select * from compare)
    '''
    inc_view_sql = []
    print( f'\n== GENERATING INC_VIEW SQL  for {len(inc_tbls)} tables')
    run_dt_str = datetime.now().strftime('%Y-%m-%d')

    # Connect to Connection1 for generating the List of Table/Columns to turn in SQL statements.
    curr_conn = get_dbcon(args, args.curr_env, args.curr )
    
    if args.debug:
        args.log.info(f'\nConnection ::CURR:: ({curr_conn.dbtype.upper()}) <== Generating SQL for tables via generate_view_sql')

    for row in inc_tbls :
        sql = ''
        db = 'INC_VIEWS'
        schema = f"{row['TABLE_SCHEMA']}"
        table_name = f"{row['TABLE_NAME']}"
        table_name = table_name.strip()

        if args.init:
            create_schema = f'CREATE SCHEMA IF NOT EXISTS {db}.{schema}'
            curr_conn.exe(  create_schema )

        sql = f''' 
create or replace view  {db}.{schema}.{table_name}_VW as (
    with mx as ( select max(bwc_dw_last_seen) as max_seen_key 
                    from INC_DEV_SOURCE.{schema}.{table_name} 
            ),
    compare as (
    select 
            case when  bwc_dw_last_seen = max_seen_key then 'Y' end as BWC_DW_CURRENT_IND
        , case when nvl(BWC_DW_CURRENT_IND ,'X') != 'Y' then 'D' 
            when bwc_dw_last_seen >  bwc_dw_load_key  then 'U' 
            when bwc_dw_load_key = bwc_dw_last_seen then 'I'
            end as BWC_DW_STATUS 
        ,* from INC_DEV_SOURCE.{schema}.{table_name}
        left join mx on (bwc_dw_last_seen = max_seen_key)    
        )
select * from compare)
        '''

        inc_view_sql.append( sql )

    return inc_view_sql


def generate_table_list( args ):
    '''
    Generate the SQL by looping through the available numeric columns, ignoring certain column_name endings and selecting metrics.
    e.g.

        with cols as (
        select table_catalog, table_schema, table_name 
        from INC_DEV_SOURCE.information_schema.columns
        where table_schema not in ('PUBLIC','INFORMATION_SCHEMA')
    )
        select table_catalog, table_schema, table_name from cols;            
    '''
    sql = ''
    schema = args.schema 

    curr_conn = get_dbcon(args, args.curr_env, args.curr )

    if args.schema:
        print( f'----\n RESTRICTED to schema: {args.schema}\n----\n' )

    curr_conn.exe( 'use secondary roles all' ) 

    dbtype = curr_conn.dbtype.lower() 
    if dbtype =='snowflake':
        sql = f'''with cols as (
            SELECT table_catalog, table_schema, table_name 
            FROM {args.curr_db}.information_schema.tables WHERE '''
        if args.schema:
            sql += f''' table_schema = {args.schema!r} '''
        else:
            sql += '''  table_schema not in ('PUBLIC','INFORMATION_SCHEMA') '''
            
        sql += f" ) " 
        sql += "select table_catalog, table_schema, table_name from cols "
        sql += "where not rlike (table_name, '.*(_BWC)[0-9]+')"

    all_args=( args, sql )
    schema = args.schema if args.schema else 'ALL SCHEMAS'
    print(f'::: Generating Table Lists for Schema: {schema}:::')   
    print(f'\n-----\n{sql}\n-----') 
    # results = run_sql(all_args)
    results =  curr_conn.fetchall( sql ) 
    
    print( f'===> {len(list(results))} table entries generated  <===\n')
        
    return results


def geterr(note = '', clean = True):
        error_msg = traceback.format_exc()
        if clean: error_msg = error_msg.replace('\n','')
        return f'ERROR: ###### {note} {error_msg}'


def run_sql(all_args):
    args, sql, idx = all_args

    curr_conn = get_dbcon(args, args.curr_env, args.curr )

    # Make sure that all rights are 
    curr_conn.exe( 'use secondary roles all' ) 
    
    try:
        result = curr_conn.exe( sql ) 
        sql_str = sql.split('\n')[1]  
        print(f"{idx:>3} => {result}: {sql_str}...")
    except:
        err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
        print( f'### ERROR ###:\n{err}')
        print( sql )

    return result, sql if result else f'ERR: {sql}'


def main():
    print('STARTING  >>>>')
    args = process_args()
    results = []

    if not args.silent:
        logging = True
    else:
        logging = False

    tables =  generate_table_list( args )
    if not tables:
        print( f'## No tables found in the Connection specified'); exit()        
 
    if args.parallel > 1:
        print(f'\n  -- Spawning {args.parallel} processes --')
    else:
        print(f'\n  -- Running Single Threaded --')

    queries =  generate_view_sql(args, tables)
    all_args = [ (args, sql, idx) for idx, sql in enumerate( queries ) ]

    if args.debug:
        print(f'--- SAMPLE INC_VIEW SQL:')
        for i in range(3):
            print( queries[i] )
    else:
        results.append( inf.run_parallel(run_sql, args = all_args, parallel = args.parallel, log = logging) )
        # Always write the output to Snowflake, but only output XLSX if a directory is specified
        
    print(f'### DONE ###')


if __name__ == '__main__':
    main() 



# cd bwcroot\bwcenv\bwcrun
# python run_create_inc_views.py --curr prd/etl_validation/INC_DEV_SOURCE  --parallel 20
# python run_create_inc_views.py --curr prd/etl_validation/INC_DEV_SOURCE  --parallel 10 --init
# python run_create_inc_views.py --curr prd/etl_validation/INC_DEV_SOURCE  --schema BASE --parallel 10 --debug

