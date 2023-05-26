from itertools import count
import sys, os, argparse, re, csv, traceback
from pathlib import Path
#other libraries
#open import openpyxl
#from tqdm import tqdm
#from openpyxl import Workbook
#from sheet2dict import Worksheet
from datetime import datetime

'''
re is used to correct the names of the tests for logging
datetime is used for format the filename
csv is used to read the CSV sql file into a dictionary and append to a list of queries
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
    etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python run_compare_schema2.py --conn1 dev/snowflake_dev --sql c:/temp/test.sql",add_help=True)

    #required
    parser.add_argument('--conn1', required=True, help='env/database (from dbsetup)')
    # parser.add_argument('--sql', required=True, help='default directory for all logs, data files')
    parser.add_argument('--schema', required=True, help='schema name for all numeric comparisons')
    
    #optional
    parser.add_argument('--conn2', required=False, help='env/database for comparison (from dbsetup)')
    parser.add_argument('--output', required=False, help='Output directory/file for query results.  Defaults to sql directory')
    parser.add_argument('--eldir', required=False, default=etldir,help='default directory for all extracts')
    parser.add_argument('--silent', required=False, default=False, action='store_true', help='include if you want to silence validation query log file generation')
    parser.add_argument('-l','--limit', required=False, default=False,  help='include if you want to limit the number of queries as a test')
    parser.add_argument('-p','--parallel', required=False, default=1, help='number of instances to spool up.')
    
    args = parser.parse_args()

    # Added to simplify copying into Snowflake
    args.snow_prefix='@~'
    args.fields = []
    
    # args.parallel = 20
    args.parallel = int( args.parallel )
    args.limit = int( args.limit ) if args.limit else False

    # args.etldir = Path( args.eldir )
    args.etldir = Path( args.output.strip() ) if args.output else Path( etldir.strip() )

    # Use the same directory for the Output as the Source SQL file, if possible.
    # args.output = Path( args.output.strip() ) if args.output else Path( args.etldir.strip() )
    if args.output:
        args.output = Path( args.output.strip() )
    
    args.conn1=args.conn1.strip('/')
    if args.conn1.count('/') == 2:
        args.conn1_env, args.conn1, args.conn1_db = args.conn1.split('/')
    else:
        args.conn1_db = None
        args.conn1_env, args.conn1 = args.conn1.split('/', 1 )

    if args.conn2:
        args.conn2=args.conn2.strip('/')
        if args.conn2.count('/') == 2:
            args.conn2_env, args.conn2, args.conn2_db = args.conn2.split('/')
        else:
            args.conn2_db = None
            args.conn2_env, args.conn2 = args.conn2.split('/', 1 )
            
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
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con = dblib.DB(tgtdb, log='', port = tgtdb.get('port',''))
    return con

def generate_columns(args):
    '''
    Generate the SQL by looping through the available numeric columns, ignoring certain column_name endings and selecting metrics.
    e.g.

        with cols as ( select * from information_schema.columns
        where table_schema = 'PCMP' and table_name = 'BWC_INTFC_EXPOSURE_DATA_OUT'
        and column_name not like '%_ID' and data_type = 'NUMBER' )
        select table_catalog, table_schema, table_name, column_name from cols;
            
    '''
    ignore_endings = ['%_ID','%_NO','%_USER_ID_%']
    # queries = []
    sql = ''
    schema = args.schema 

    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    dbtype = db1_conn.dbtype.lower() 
    if dbtype =='snowflake':
        sql = f"with cols as (SELECT * FROM {args.conn1_db}.information_schema.columns WHERE table_schema = {schema!r} "
        for v in ignore_endings:
            sql += f" AND column_name not like {v!r} " 
        sql += " AND data_type = 'NUMBER' ) "
        sql += "select table_catalog, table_schema, table_name, column_name from cols "
        # print(f'SQL generated: {sql}'); input('go')

    all_args=( args, sql )
    print(f'::: Generating Validation Column List from Schema: {args.schema}:::')   
    print(f'\n-----\n{sql}\n-----') 
    # results = run_sql(all_args)
    results =  db1_conn.fetchall( sql ) 
    
    print( f'===> {len(list(results))} columns generated  <===\n')
        
    return results


def generate_queries(args, inc_cols):
    DB_DICT = {}
    print( f'== GENERATING QUERIES from {len(inc_cols)} columns')
    run_dt_str = datetime.now().strftime('%Y-%m-%d')

    # Connect to Connection1 for generating the List of Table/Columns to turn in SQL statements.
    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    if args.conn2:
        db2_conn = get_dbcon(args, args.conn2_env, args.conn2 )
    
    if not args.silent:
        args.log.info(f'Connection 1 ({db1_conn.dbtype.upper()}) <== Generating SQL for columns via generate_queries')

    # sql2run = sf_sql if db1_conn.dbtype.lower() =='snowflake' else qsql 
    # TODO will this fetchdict not work if Oracle is the first connection
    # row_gen = db1_conn.fetchdict( sql2run )

    agg_types = {'count':4, 'min':5, 'max':6, 'avg':7, 'sum':8}

    # Build SQL for conn1/conn2 to parallelize
    for col_num, row in enumerate(inc_cols):
        conn_from = ''
        # {'TABLE_CATALOG': 'UAT_SOURCE', 'TABLE_SCHEMA': 'PCMP', 'TABLE_NAME': 'ACTIVITY', 'COLUMN_NAME': 'AUDIT_USER_ID_CREA'}
        runsql = f"select {run_dt_str!r} as RUN_DT, '{row['TABLE_CATALOG']}' as DB_NAME, '{row['TABLE_SCHEMA']}' as TBL_SCHEMA, '{row['TABLE_NAME']}' as TBL_NAME,  '{row['COLUMN_NAME']}' as COL_NAME "
        for each, _col in agg_types.items():
            if each == 'avg':
                runsql += f",  trunc( {each}( {row['COLUMN_NAME']} ), 2) as {each.upper()}"
            else:
                runsql += f", {each}( {row['COLUMN_NAME']} ) as {each.upper()}"
        if  db1_conn.dbtype.lower() =='snowflake' :
            conn1_db = args.conn1_db if args.conn1_db else row['TABLE_CATALOG']       
            conn_from = f"    from {conn1_db}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}  "
            # conn_from = f"    from {row['TABLE_CATALOG']}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}  "
        else:
            conn_from = f"    from {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}  "

        row['DB1_SQL'] = runsql + conn_from

        if args.conn2:
            runsql = f"select {run_dt_str!r} as RUN_DT, '{row['TABLE_CATALOG']}' as DB_NAME, '{row['TABLE_SCHEMA']}' as TBL_SCHEMA, '{row['TABLE_NAME']}' as TBL_NAME,  '{row['COLUMN_NAME']}' as COL_NAME "
            for each, _col in agg_types.items():
                if each == 'avg':
                    runsql += f",  trunc( {each}( {row['COLUMN_NAME']} ), 2) as {each.upper()}"
                else:
                    runsql += f", {each}( {row['COLUMN_NAME']} ) as {each.upper()}"
            if  db2_conn.dbtype.lower() =='snowflake' :
                conn2_db = args.conn2_db if args.conn2_db else db2_conn.dbname
                # conn2_db = db2_conn.dbname
                conn_from = f"    from {conn2_db}.{row['TABLE_SCHEMA']}.{row['TABLE_NAME']}  "
            else:
                conn_from = f"    from {row['TABLE_SCHEMA']}.{row['TABLE_NAME']}  "
            
            row['DB2_SQL'] = runsql + conn_from

    return inc_cols


def run_sql(all_args):
    args, row, idx = all_args
    # compare_results = []

    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    if args.conn2:
        db2_conn = get_dbcon(args, args.conn2_env, args.conn2 )

    agg_types = {'count':5, 'min':6, 'max':7, 'avg':8, 'sum':9}

    # for i in tqdm (range(1), desc = f"Executing {db1_conn.dbtype.upper()} SQL..." ):
    try:
        #for i in tqdm (range(1), desc=f"Executing {args.conn1_env} - {db1_conn.dbtype.lower()} SQL ({idx})..." ):
        result = db1_conn.fetchone( row['DB1_SQL'] )
            # result = db1_conn.fetchdict( runsql + conn_from  )

        if result == None:
            result = '~'

        row['RUN_DT'] = result.get('RUN_DT', datetime.now().strftime('%Y-%m-%d') )
        
        # Oracle fetch does not return column dict inforation (headers)
        for each, col in agg_types.items():
            if db1_conn.dbtype.lower() == 'oracle':
                row[f'C1_{each}'] =  result[col] 
            else:
                row[f'C1_{each}'] =  result.get(each.upper() ) 

        row['C1'] =  args.conn1_env +'_'+ db1_conn.dbtype.lower()
        if not args.silent: args.log.info(f"{row['C1']} RESULTS: {result}")

        row['CONN1_ERR'] = ''

        if args.conn2:
            row['CONN2_ERR'] = ''
            #for i in tqdm (range(1), desc=f"Executing {args.conn2_env} - {db2_conn.dbtype.lower()}  SQL ({idx})..." ):
                        
            row['C2'] =  args.conn2_env +'_'+ db2_conn.dbtype.lower()
            if not args.silent: args.log.info(f"{row['C2']} RESULTS: {result}")

        # compare_results.append( row.copy() )
        # row['C1'] =  args.conn1_env +'_'+ db1_conn.dbtype.lower()
    except:
        err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
        if "ProgrammingError" in err:
            row['CONN1_ERR'] = f"ERR: {err.split('ProgrammingError: ')[1]}"
        elif "DatabaseError" in err:
            row['CONN1_ERR'] = f"ERR: {err.split('DatabaseError: ')[1]}"
        else:
            row['CONN1_ERR'] = err
        # continue

    # if idx>2: break
    if not args.silent:
        print( row['DB1_SQL'])
        print( row )
        
    return row


def geterr(note = '', clean = True):
        error_msg = traceback.format_exc()
        if clean: error_msg = error_msg.replace('\n','')
        return f'ERROR: ###### {note} {error_msg}'


def snow_put(args,  comparison_file, stagedir='', path=''):
    '''
        requires the full path
    put file://C:\temp\PCMP--uat_snowflake_vs_uat_oracle_comparison_20220819.csv @~/DEV_EDW/PUBLIC/SCHEMA_COMPARE/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';

    '''
    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    path = f'{args.etldir}/{comparison_file}'
    
    stagedir = 'DEV_EDW/PUBLIC/SCHEMA_COMPARE/'

    # Clean out staging before uploading new results
    remove_sql = 'rm @~/DEV_EDW/PUBLIC/SCHEMA_COMPARE/'
    print(remove_sql)
    rm_result = db1_conn.exe(remove_sql)
    if rm_result:
        print( f'## REMOVED FILES FROM STAGE...' )

    stage_cmd=f'''put file://{path} {args.snow_prefix}/{stagedir} auto_compress=true;'''
    print(stage_cmd)
    result = db1_conn.exe(stage_cmd)
    staged_files = snow_list_stage(args, db1_conn, stagedir )
    
    if not staged_files:
        raise Warning(f'Missing staged file: {path}')
    # args.log.info(f'Staged {staged_files}')
    print(f'\n-- Staged {staged_files}')
    return staged_files


def snow_list_stage(args, db1_conn, stagedir=''):
    '''
        https://docs.snowflake.com/en/sql-reference/sql/list.html

    list @~/DEV_EDW/PUBLIC/SCHEMA_COMPARE/;
    [{'name': 'DEV_EDW/PUBLIC/SCHEMA_COMPARE/PCMP--uat_snowflake_vs_uat_oracle_comparison_20220819.csv.gz', 'size': 688, 'md5': 'f500ff108c48d4339ffab8a8f070fb9e', 'last_modified': 'Fri, 19 Aug 2022 18:01:57 GMT'}]   
    '''
    sql=f'list {args.snow_prefix}/{stagedir}; '

    stage_files=[ row for  row in db1_conn.fetchdict(sql) ]
    return stage_files


def snow_copy_into(args, stagedir, comparison_file ):
    '''
    if the copy into doesn't work, we may need to start using force=TRUE to force it to reload.  If the results of the run are the same 
    as the previous execution, snowflake will assume it has already processed the file and not load it unless forced.'''
    
    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    file_format = f"""file_format =  (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' )  """
    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""
    fields = ', '.join( args.fields )
    selectstmt= ''
    stmt = []

    for x in range( len(args.fields) ):
        stmt.append( f's.${x+1}')
    selectstmt = str( ', '.join(stmt))

    # copy_cmd=f"""copy into {dbname}.{schema}.{table} from {args.snow_prefix}/{stagedir} {file_format} on_error='continue'; """
    copy_cmd = f"""copy into DEV_EDW.PUBLIC.SCHEMA_COMPARISON ( {fields} ) from (select {selectstmt} from {args.snow_prefix}{stagedir}{comparison_file} s) {file_format} TRUNCATECOLUMNS= TRUE on_error='continue'; """
    # args.log.info(copy_cmd)
    print(f' --COPYING via {copy_cmd}' )
    result=list( db1_conn.exe(copy_cmd))
    # args.log.info(str(result))
    print(str(result))
    return result[0]


def insert_into_sf(args, results):
    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    insert_sql = []
    for rw in results[0]:
        sql = 'insert into DEV_EDW.PUBLIC.SCHEMA_COMPARISON '
        fields = ', '.join(rw.keys() )
        sql += f'( {fields} ) '

        values = ', '.join( ["'{}'".format(str(item).replace("'", "''" )) for  item in rw.values() ])
        values = values.replace("'None'", "Null")

        sql += f' values ({values}) '

        insert_sql.append(sql)

    #for sql in tqdm ( insert_sql, total= len(insert_sql), desc=f"Inserting Results into Snowflake:  DEV_EDW.PUBLIC.SCHEMA.COMPARISON..." ):    
        # for sql in insert_sql:
            # print( f' # Executing: {sql}' )
        # def insert_many(self,db,schema,table,row_gen,insert_size=2000):
    db1_conn.exe( sql, commit=True  )
    

def saveresults( args, results ):
    drctry = Path( args.output ) if args.output else Path( args.etldir )
    drctry.mkdir( parents=True, exist_ok = True )
    
    now = datetime.now()
    datestring = now.strftime("%Y%m%d")

    result_file = args.schema.upper()
    fields_avail = results[0][0]
    
    compare = fields_avail['C1']+'_vs_'+fields_avail['C2'] if fields_avail['C2'] else fields_avail['C1']
    # compare = f"{args.conn1.replace('/','_')}_vs_{args.conn2.replace('/','_')}" if args.conn2 else f"{args.conn1.replace('/','_')}"    
    validation_file = f'{result_file}-{compare}_comparison_{datestring}.xlsx'

    if not args.silent:
        args.log.info(f'Saving the output to {drctry}/{validation_file}')
    else:
        print(f'\n\nSaving the output to {drctry}/{validation_file}')

    # Output the results of the audit queries to a worksheet via openpyxl
    if False:
        wb = Workbook()
        ws = wb.active

        # Write Headers
        headers = [ 'CATALOG', 'SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'C1_SQL','C2_SQL', 'RUN_DT' ]
        result_cols = [ 'C1_COUNT', 'C1_MIN', 'C1_MAX', 'C1_AVG', 'C1_SUM', 'C1', 'CONN1_ERR' ]
        headers.extend( result_cols )
        if args.conn2:
            conn2_cols = [ 'CONN2_ERR','C2_COUNT', 'C2_MIN', 'C2_MAX', 'C2_AVG', 'C2_SUM', 'C2' ]
            headers.extend( conn2_cols )

        ws.append( headers )

        # Write Data
        #for i in tqdm (range(1), desc="Saving..."):
        for row in results[0]:
            output = list( row.values() )
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

    now = datetime.now()
    datestring = now.strftime("%Y%m%d")

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

    # TODO Instead of getting a list of queries, connect to the database and find all numeric, non-ID columns in a schema
    # Create Queries for count, sum, min, max values for each schema/table/column combination.    
    # query_list = generate_queries(args)
    columns =  generate_columns(args)
    queries =  generate_queries(args, columns)

    queries_to_run = []
    if args.limit:
        print( f'--- Limiting to {str(args.limit)} SQL executions...')
        for i in range(0, args.limit ):
            queries_to_run.append( queries[i] )
    else: 
        print( f'--- Running against all columns...')
        queries_to_run = queries

    all_args = [ (args, sql, idx) for idx, sql in enumerate(queries_to_run) ] 

    if args.parallel > 1:
        print(f'\n  -- Spawning {args.parallel} processes --')
    else:
        print(f'\n  -- Running Single Threaded --')
    # results = inf.run_parallel(run_sql, args = all_args, parallel = args.parallel, log = logging)    
    results.append( inf.run_parallel(run_sql, args = all_args, parallel = args.parallel, log = logging)   ) 

    # Always write the output to Snowflake, but only output XLSX if a directory is specified
    if args.output:
        if not args.silent:
            args.log.info(f'### SAVING RESLTS ###')
        else:
            print(f'### SAVING RESULTS ###')

        saveresults( args, results )

    comparison_file = writecsv( args, results )

    print(f'### LOADING REUSLTS TO SNOWFLAKE ###')

    stagedir = '/DEV_EDW/PUBLIC/SCHEMA_COMPARE/'
    snow_put(args, comparison_file, stagedir, path=args.etldir )
    snow_copy_into( args, stagedir, comparison_file )
            
    # insert_into_sf( args, results )

    if not args.silent:
        args.log.info(f'### DONE ###')
    else:
        print(f'### DONE ###')
    
if __name__ == '__main__':
    main() 



# cd bwcroot\bwcenv\bwcrun
# python run_compare_schema2.py --conn1 prd/vertica_testprd --conn2 prd/snowflake_prd  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3
# python run_compare_schema2.py --conn1 prd/vertica_testprd --conn2 prd/snowflake_prd  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3 --silent --output c:/Temp/TestOutput
# python run_compare_schema2.py --conn1 prd/snowflake_prd  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3 --silent --output c:/Temp/TestOutput

# python run_compare_schema2.py --conn1 prd/snowflake_prd --conn2 /dev/snowflake_dev  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3 --silent --output c:/Temp/TestOutput
# python run_compare_schema2.py --conn1 dev/snowflake_dev --schema DW_REPORT  --limit 10 --parallel 10 --silent 
# python run_compare_schema2.py --conn2 uat/oracle_etl --conn1 uat/snowflake_uat  --schema PCMP  --parallel 1 --limit 200 --silent --output c:/Temp
# python run_compare_schema2.py --conn2 uat/oracle_etl --conn1 uat/snowflake_uat --schema PCMP --silent --parallel 1 --limit 20 --output C:/temp
# python run_compare_schema2.py --conn2 uat/oracle_etl --conn1 uat/snowflake_uat --schema PCMP --silent --parallel 10 --limit 100 --output C:/temp
# python run_compare_schema2.py --conn2 uat/oracle_etl --conn1 uat/snowflake_uat --schema PCMP --silent --parallel 20 --output C:/temp
# python run_compare_schema2.py --conn1 prd/snowflake_prd --conn2 uat/snowflake_uat  --schema PCMP --parallel 15 --silent --output c:/Temp/Schema2Output 

# python run_compare_schema2.py --conn1 prd/snowflake_prd/PRD_SOURCE --conn2 uat/snowflake_uat/UAT_SOURCE  --schema PCMP --p 15 --silent --l 15 --o c:/Temp/Schema2Output 
# python run_compare_schema2.py --conn1 prd/snow_etl/RPD1 --conn2 prd/vertica_etl/RPD1  --schema DW_REPORT --p 20 --silent --o e:/Temp/Schema2Output 