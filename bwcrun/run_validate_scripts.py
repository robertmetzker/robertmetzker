import sys, os, argparse, re, csv, traceback
from pathlib import Path
# from openpyxl import Workbook       # for saving as xlsx
# from sheet2dict import Worksheet
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
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python run_validate_scripts.py --conn1 dev/snowflake_dev --sql c:/temp/test.sql",add_help=True)

    #required
    parser.add_argument('--conn1', required=True, help='env/dbkey/db (from dbsetup)')
    parser.add_argument('--sql', required=True, help='default directory for all logs, data files')
    
    #optional
    parser.add_argument('--conn2', required=False, help='env/dbkey/db for comparison (from dbsetup)')
    parser.add_argument('--output', required=False, help='Output directory/file for query results.  Defaults to sql directory')
    parser.add_argument('--eldir', required=False, default=etldir,help='default directory for all extracts')
    parser.add_argument('--saveto', required=False, default='BWC_AUDIT.PUBLIC.BWC_ETL_VALIDATION_RESULTS', help='db.schema.table to output the results to- default: BWC_AUDIT.PUBLIC.BWC_ETL_VALIDATION_RESULTS')
    parser.add_argument('--reset', required=False, default=False, action='store_true', help='include if you want to re-process and clear previously stored results')
    parser.add_argument('-s','--silent', required=False, default=False, action='store_true', help='include if you want to silence validation query log file generation')
    parser.add_argument('-l','--limit', required=False, default=False,  help='include if you want to limit the number of queries as a test')
    parser.add_argument('-p','--parallel', required=False, default=1, help='number of instances to spool up.')
    parser.add_argument('--xls', required=False, default=False,action='store_true' ,help='Add this to save results to Excel')
    
    args = parser.parse_args()

    # args.parallel = 20
    args.parallel = int( args.parallel )
    args.limit = int( args.limit ) if args.limit else False

    args.snow_prefix='@~'

    # args.etldir = Path( args.eldir )
    args.etldir = Path( args.output ) if args.output else Path( etldir )

    # Use the same directory for the Output as the Source SQL file, if possible.
    args.output = Path( args.output ) if args.output else Path( os.path.splitext( Path( args.sql ))[0] )

    args.conn1=args.conn1.strip('/')
    if args.conn1.count('/') == 1:
        args.conn1_env, args.conn1 = args.conn1.split('/', 1 )
    elif args.conn1.count('/') == 2:
        args.conn1_env, args.conn1, args.srcdb =args.conn1.split('/')

    if args.conn2:
        args.conn2 = args.conn2.strip('/')
        args.conn2_env, args.conn2 = args.conn2.split('/', 1 )
            
    if not args.silent:
        args.logdir = args.output/'logs'
        args.log = inf.setup_log(args.logdir, app = 'parent')
        args.log.info( f'processing in {args.eldir}' )
        print( f'\t=== Using ETLDIR: {etldir}' )
        print( f'--Processing in: {args.eldir}' )

    args.stagedir = '/BWC_AUDIT/PUBLIC/BWC_ETL_VALIDATION/'
    if not args.srcdb: args.srcdb = ""

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

    
def get_val_queries(args):
    """Validation Queries were in Vertica_DEV, but we replicate from Vertica Prod_Weekly.
    The queries also exist in Snowflake, but not in Vertica Prod or Prod_Weekly."""

    current_row = 0
    queries = []
    # Arbitrary limit of 10_000 if not specified as an argument
    limit = int( args.limit ) if args.limit else 10000

    print(f'::: Fetching Validation Queries :::')    
    if args.sql: 
        file2read = args.sql
        data = csv.DictReader(open( file2read ))
        print(f"=== Reading CSV file as a dictionary:  {file2read}\n")
        for row in data:
            queries.append( row.copy() )
            current_row += 1
            if current_row > limit: break
    else:
        print('### ERROR:  Nothing to read')

    # Check to see if the minimum columns are in the query file...
    reqd_cols = set(['CNTRL_TOTAL_ID', 'QUERY_TYPE', 'CNTRL_TOTAL_NAME', 'CNTRL_TOTAL_SUB_NAME', 'VAL_SQL' ])
    keys = []
    for key in queries:
        keys.extend( key )
    file_keys = set( keys )

    if reqd_cols.difference( file_keys):
        print( '### MISSING KEYS' )
        print( f'  IN FILE : {file_keys}\n  EXPECTED: {reqd_cols}')
        print( f'## Missing: { reqd_cols.difference( file_keys)} ')
        exit()
    else:
        print('--- ALL KEYS found')
        print( f'FILE_KEYS: {file_keys}\nEXPECTED: {reqd_cols}')

    query_list=[]
    for _idx, row in enumerate( queries ):
        # {'CNTRL_TOTAL_ID': 10003, 'QUERY_TYPE': 'DW_REPORT', 'VAL_SQL': 'select count(DISTINCT CLM_AGRE_ID) from (select CLM_AGRE_ID, INDST_CD_EFF_DT from DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY group by 1,2 having count(*)>1)x'}
        # {'ETL_CREATE_DTM': '2022-03-31T13:16:29', 'VERSION_DT': '2022_03_31', 'CNTRL_TOTAL_ID': '61005', 'CNTRL_TOTAL_NAME': 'DW_POLICY_PERIOD_BILLING_SCH_HIST', 'CNTRL_TOTAL_SUB_NAME': 'Sum of Current Scheduled Amounts', 'QUERY_TYPE': 'DW_REPORT', 'VAL_SQL': "select SUM(nvl(PLCY_PRD_BILL_SCH_DRV_SCH_AMT,0)) from dw_report.DW_POLICY_PERIOD_BILLING_SCH_HIST where CRNT_PLCY_PRD_BILL_SCH_IND = 'y' and HIST_END_DTM is null "}
        row_sql = {}
        row_sql['CNTRL_TOTAL_ID'] = row['CNTRL_TOTAL_ID']
        row_sql['QUERY_TYPE'] = row['QUERY_TYPE']   
        row_sql['CNTRL_TOTAL_NAME'] = row['CNTRL_TOTAL_NAME']
        row_sql['CNTRL_TOTAL_SUB_NAME'] = row['CNTRL_TOTAL_SUB_NAME']
        row_sql['CONN1_RESULTS'] = ''
        row_sql['CONN2_RESULTS'] = ''
        row_sql['VAL_SQL'] = row['VAL_SQL']
        # if args.srcdb:
        #     qry = row['VAL_SQL']
        #     onpos = qry.find(' on ')
        #     if onpos > 0:
        #         row_sql['VAL_SQL'] = re.sub( '(\w+\.\w+)', f'{args.srcdb}.\\1 ', qry[:onpos] ) + qry[onpos:]
        #     else:
        #         row_sql['VAL_SQL'] = re.sub( '(\w+\.\w+)', f'{args.srcdb}.\\1 ', qry )
                
        # else:
        #     row_sql['VAL_SQL'] = row['VAL_SQL']
        row_sql['ERR_MSG'] = ''
        query_list.append( row_sql.copy() )
        
    # for row in enumerate( query_list ):
    #     print(row); input('go')
    
    return query_list


def convert_sf_sql(qsql):
    '''Clean up SYSDATE, BTRIM, '''
    if 'sysdate' in qsql.lower():
        sf_sql = re.sub('sysdate|SYSDATE','current_date', qsql)
    else: 
        sf_sql = qsql

    sf_sql = re.sub('BTRIM|btrim','TRIM', sf_sql )
    sf_sql = re.sub('trunc\(|TRUNC\(',"TRUNC('DAY',", sf_sql )
    return sf_sql


def clean_sql(qsql):
    '''
    This function cleans up field strings that are resserved words for most databases by writing it in UPPERCASE and making sure it has spaces on both sides
    NOTE:  It is important to check for trailing spaces in case the string is part of another word
    '''
    # problem_statements = [ 'FROM ','AND ','UNION','MINUS','JOIN ','INNER ','OUTER ','LEFT ','RIGHT ','SELECT','WHERE ','HAVING ', 'GROUP ' ]
    problem_statements = [ 'FROM ','AND ','UNION','MINUS','JOIN ','INNER ','OUTER ','LEFT ','RIGHT ','WHERE ','HAVING ', 'GROUP ' ]

    for problem in problem_statements:
        if problem.lower() in qsql.lower():
            problem_LC = problem.lower()
            problem_UC = problem.upper()
            qsql = re.sub(rf'{problem_LC}|{problem_UC}',rf' {problem_UC} ', qsql )

    return qsql


def geterr(note = '', clean = True):
        error_msg = traceback.format_exc()
        if clean: error_msg = error_msg.replace('\n','')
        return f'ERROR: ###### {note} {error_msg}'


def snow_put(args,  comparison_file, stagedir='', path=''):
    '''
        requires the full path
    put file://C:\temp\PCMP--uat_snowflake_vs_uat_oracle_comparison_20220819.csv @~/BWC_AUDIT/PUBLIC/SCHEMA_COMPARE/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';

    '''
    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    path = f'{args.etldir}/{comparison_file}'
    
    stagedir = args.stagedir

    # Clean out staging before uploading new results
    remove_sql = f'rm @~{stagedir}'
    print(remove_sql)
    rm_result = db1_conn.exe(remove_sql)
    if rm_result:
        print( f'## REMOVED FILES FROM STAGE...' )

    stage_cmd=f'''put file://{path} {args.snow_prefix}{stagedir} auto_compress=true;'''
    print( stage_cmd )
    result = db1_conn.exe( stage_cmd )
    staged_files = snow_list_stage( args, db1_conn, stagedir )
    
    # if not staged_files:
    #     raise Warning(f'Missing staged file: {path}')
    # args.log.info(f'Staged {staged_files}')

    print(f'\n-- Staged {staged_files}')
    return staged_files


def snow_list_stage(args, db1_conn, stagedir=''):
    '''
        https://docs.snowflake.com/en/sql-reference/sql/list.html

    list @~/BWC_AUDIT/PUBLIC/BWC_ETL_VALIDATION/;
    [{'name': 'BWC_AUDIT/PUBLIC/BWC_ETL_VALIDATION--uat_snowflake_vs_uat_oracle_comparison_20220819.csv.gz', 'size': 688, 'md5': 'f500ff108c48d4339ffab8a8f070fb9e', 'last_modified': 'Fri, 19 Aug 2022 18:01:57 GMT'}]   
    '''
    stagedir = args.stagedir
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

    stagedir = args.stagedir
    # copy_cmd=f"""copy into {dbname}.{schema}.{table} from {args.snow_prefix}/{stagedir} {file_format} on_error='continue'; """
    copy_cmd = f"""copy into {args.saveto} ( {fields} ) from (select {selectstmt} from {args.snow_prefix}{stagedir}{comparison_file} s) {file_format} TRUNCATECOLUMNS= TRUE on_error='continue'; """
    # args.log.info(copy_cmd)
    print(f' --COPYING via {copy_cmd}' )
    result=list( db1_conn.exe(copy_cmd) )
    # args.log.info(str(result))
    print( str(result) )
    return result[0]


# def insert_into_sf(args, results):
#     db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
#     insert_sql = []
#     for rw in results[0]:
#         sql = f'insert into {args.saveto} '
#         fields = ', '.join(rw.keys() )
#         sql += f'( {fields} ) '

#         values = ', '.join( ["'{}'".format(str(item).replace("'", "''" )) for  item in rw.values() ])
#         values = values.replace("'None'", "Null")

#         sql += f' values ({values}) '
#         insert_sql.append(sql)

#     #for sql in tqdm ( insert_sql, total= len(insert_sql), desc=f"Inserting Results into Snowflake:  BWC_AUDIT.PUBLIC.SCHEMA.COMPARISON..." ):    
#         # for sql in insert_sql:
#             # print( f' # Executing: {sql}' )
#         # def insert_many(self,db,schema,table,row_gen,insert_size=2000):
#     db1_conn.exe( sql, commit=True  )


def writecsv( args, results ):
    print( '\n--- Writing CSV ---')

    # unpack the list of lists for write_csv from infrastructure to use
    csv_list = []
    for rw in results[0]:
        csv_list.append( rw )

    datestring = datetime.now().strftime("%Y%m%d")

    fields_avail = csv_list[0]

    result_file = Path(args.sql).stem    
    comparison_file = f'{result_file}_results_{datestring}.csv'

    # Correct the SQL columns (DB1_SQL, DB2_SQL) to change ' to '' for inserting into SF
    for row in results:
        row['VAL_SQL'] = row['VAL_SQL'].replace("'","''")
    
    args.fields = list( results[0].keys() )
    print(f'**** FIELDS: {args.fields}')
    # write_csv(fname,rows,raw=False,delim='\t',term='\n',prefix='',sortit=True,log=None,verify=False):
    with open( args.etldir/comparison_file, 'w', newline = '') as file2write:
        with file2write:
            csvoutput = csv.DictWriter( file2write, args.fields )
            csvoutput.writeheader()
            csvoutput.writerows( results )

    # inf.write_csv( args.etldir/comparison_file, csv_list )
    print( f' --- Wrote CSV file to: {args.etldir}/{comparison_file}\n')

    return comparison_file


def run_sql(all_args):
    args, sql = all_args
    captured_errors = []
    err_dict = {'QRY_ID':'', 'ERR_MSG':''}

    qauditid = sql['CNTRL_TOTAL_ID']
    qtype = sql['QUERY_TYPE']
    qcontrol = sql['CNTRL_TOTAL_SUB_NAME']
    qcleanname = re.sub('[^0-9a-zA-Z ]+','',qcontrol)

    qsql = clean_sql(sql['VAL_SQL'])
    # TODO - Add check to see if connection requires cleaning of SQL for Snowflake
    sf_sql = convert_sf_sql( qsql )
    
    if not args.silent:
        if '.' in sql:
            logname = sql.split('.')[-1].split()[0]
        logname = f'{qtype}_{qauditid}_{qcleanname}'
        args.log = inf.setup_log(args.logdir, app = f'child_{logname}')
        args.log.info(f'processing in {args.etldir}')

    if not args.silent:
        args.log.info(f'--- SQL:\n{qsql}')
        
    # Connect to Vertica and Run Queries
    db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
    
    if not args.silent:
        args.log.info(f'Connection 1 ({db1_conn.dbtype.upper()}) <== in run_sql')

    sql2run = sf_sql if db1_conn.dbtype.lower() =='snowflake' else qsql 
    db1_conn.fetchone(f"USE database {args.srcdb}") #If you want to use different DataBase for example DWH_DEV
    row_gen = db1_conn.fetchdict( sql2run )
    
    # for i in tqdm (range(1), desc = f"Executing {db1_conn.dbtype.upper()} SQL ({qauditid})..." ):
    try:
        for idx,row in enumerate(row_gen):
            print(  f"Executing {db1_conn.dbtype.upper()} SQL ({qauditid})..." )
            # TODO: Handle results which are more than a single column--
            # Noise here due to the keys coming back in various forms...
            try:
                result = list(row.values())[0]
            except Exception as e:
                if not args.silent: args.log.error(f'RESULTS: {e}')
                
            if result == None:
                result = '~'
            sql['CONN1_RESULTS'] = result
            if not args.silent: args.log.info(f'RESULTS: {result}')

            if idx>2: break
            if not args.silent:
                print(sql)
    except:
        err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
        sql['CONN1_RESULTS'] = '# ERR'
        if "ProgrammingError" in err:
            sql['ERR_MSG'] = f"CON1_ERR: {err.split('ProgrammingError: ')[1]}"
        elif "DatabaseError" in err:
            sql['ERR_MSG'] = f"CON1_ERR: {err.split('DatabaseError: ')[1]}"
        else:
            sql['ERR_MSG'] = err

    # sql['C1'] =  args.conn1_env +'_'+ db1_conn.dbtype.lower()
        
    if args.conn2:
        # Connect to Snowflake and Run Queries
        db2_conn = get_dbcon(args, args.conn2_env, args.conn2 )
        if not args.silent:
            args.log.info( f'CONNECTION 2 ({db2_conn.dbtype.upper()}) <== in run_sql')

        sql2run = sf_sql if db2_conn.dbtype.lower() =='snowflake' else qsql 
        row_gen = db2_conn.fetchdict( sql2run )

        # for i in tqdm (range(1), desc = f"Executing {db2_conn.dbtype.upper()} SQL ({qauditid})..." ):
        try:
            for idx, row in enumerate(row_gen):
                print(  f"Executing {db1_conn.dbtype.upper()} SQL ({qauditid})..." )
                # TODO: Handle results which are more than a single column--
                # Noise here due to the keys coming back in various forms...
                result = list(row.values())[0]
                if result == None:
                    result = '~'
                sql['CONN2_RESULTS'] = result
                if not args.silent: args.log.info(f'RESULTS: {result}')

                if idx>2: break
                if not args.silent:
                    print(sql)
        except:
            err = geterr()    #     if not args.silent: args.log.error(f'RESULTS: {e}')
            sql['CONN2_RESULTS'] = '# ERR'
            if "ProgrammingError" in err:
                sql['ERR_MSG'] = f"CON2_ERR: {err.split('ProgrammingError: ')[1]}"
            elif "DatabaseError" in err:
                sql['ERR_MSG'] = f"CON2_ERR: {err.split('DatabaseError: ')[1]}"
            else:
                sql['ERR_MSG'] = err

        # sql['C2'] = args.conn2_env +'_'+ db2_conn.dbtype.lower()
        
    return sql


def saveresults( args, results ):
    from openpyxl import Workbook       # for saving as xlsx
    drctry = Path( args.output) if args.output else Path( args.sql.parent )
    drctry.mkdir( parents=True, exist_ok = True )
    
    now = datetime.now()
    datestring = now.strftime("%Y%m%d")

    result_file = Path(args.sql).stem    
    validation_file = f'{result_file}_results_{datestring}.xlsx'

    if not args.silent:
        args.log.info(f'Saving the output to {drctry}/{validation_file}')
    else:
        print(f'\n\nSaving the output to {drctry}/{validation_file}')

    # Output the results of the audit queries to a worksheet via openpyxl
    wb = Workbook()
    ws = wb.active

    # Write Headers
    headers = ['CNTRL_TOTAL_ID', 'QUERY_TYPE', 'CNTRL_TOTAL_NAME', 'CNTRL_TOTAL_SUB_NAME','CONN1_RESULTS','CONN2_RESULTS','VAL_SQL', 'ERR_MSG']
    
    get_conn_info = results[0]
    # headers = [ h.replace('CONN1_RESULTS', get_conn_info['C1']) if h =='CONN1_RESULTS' else h for h in headers ]
    headers = [ h for h in headers ]
    # if args.conn2:
        # headers = [ h.replace('CONN2_RESULTS', get_conn_info['C2']) if h =='CONN2_RESULTS' else h for h in headers ]

    ws.append( headers )

    # Write Data
    print( "Saving..." )
    for row in results:
        row['VAL_SQL'] = row['VAL_SQL'].replace('\x1a','')
        #row.pop('C1'); 
        #if args.conn2: 
        #    row.pop('C2')
        output = list( row.values() )
        
        # TODO:  Cleanup of VAL_SQL for 60053/60054 with invalid characters...
        try:
            ws.append( output )
        except:
            row['VAL_SQL'] = '-- See Source for Actual SQL ---'
            try:
                ws.append( output )
            except:
                pass
                
    print( f'>> Saved {drctry}/{validation_file}')
    wb.save(f'{drctry}/{validation_file}')


def main():

    print('STARTING  >>>>')
    args = process_args()
        
    query_list = get_val_queries(args)
    qrycnt = len(query_list)

    if not args.silent:
        print(f'\n----- QUERIES FOUND (sample) -----\n')
        for row in query_list:
            if row['QUERY_TYPE'] == 'OTHER':
                print(row)

    print(f'\n---- Executing Audits ----')
    all_args=[( args, sql) for sql in query_list]
    
    if not args.silent:
        logging = True
    else:
        logging = False

    if args.parallel > 1:
        print(f'  -- Spawning {args.parallel} processes --')
    else:
        print(f'  -- Running Single Threaded --')
        

    results = inf.run_parallel(run_sql, args = all_args, parallel = args.parallel, log = logging)    
   # Always write the output to Snowflake, but only output XLSX if a directory is specified
    if args.output:
        if not args.silent:
            args.log.info(f'### SAVING RESLTS ###')
        else:
            print(f'### SAVING RESULTS ###')
        if args.xls:
            try:            
                saveresults( args, results )
            except:
                pass
            
    comparison_file = writecsv( args, results )

    print(f'### LOADING REUSLTS TO SNOWFLAKE ###')

    print(comparison_file, args.stagedir, args.etldir )
    snow_put(args, comparison_file, args.stagedir, path = args.etldir )

    if args.reset:
        db1_conn = get_dbcon(args, args.conn1_env, args.conn1 )
        reset_sql = f"delete from {args.saveto} where run_dt = current_date "
        result = db1_conn.fetchone( reset_sql )
        print( result )
        
    snow_copy_into( args, args.stagedir, comparison_file )
            
    # insert_into_sf( args, results )


    if not args.silent:
        args.log.info(f'### DONE ###')
    else:
        print(f'### DONE ###')
        print(f'-- Ran with: {" ".join( sys.argv ) }')
    
if __name__ == '__main__':
    main() 



# Running Validations in Snowflake and writing out to BWC_ETL_VALIDATION_RESULTS table.
# python run_validate_scripts.py --conn1 prd/snow_etl/RPD1  --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 15  --output c:/TEMP --saveto BWC_AUDIT.PUBLIC.BWC_ETL_VALIDATION_RESULTS --reset --silent --xls 
# python run_validate_scripts.py --conn1 prd/snow_etl/RPD1  --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 15  --output c:/TEMP --limit 30  --xls 

# python run_validate_scripts.py --conn1 prd/snow_etl/RPD1  --sql C:/Temp/RPD1_MAXDATES.csv  --parallel 15  --output c:/TEMP --saveto BWC_AUDIT.PUBLIC.MAX_DATE_RESULTS --reset  --silent --xls 


# Comparing Across environments...
# python run_validate_scripts.py --conn1 prd/vertica_testprd --conn2 prd/snowflake_prd  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3
# python run_validate_scripts.py --conn1 prd/vertica_testprd --conn2 prd/snowflake_prd  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3 --silent --output c:/Temp/TestOutput
# python run_validate_scripts.py --conn1 prd/snowflake_prd  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3 --silent --output c:/Temp/TestOutput

# python run_validate_scripts.py --conn1 prd/snowflake_prd --conn2 /dev/snowflake_dev  --limit 20 --sql C:/Temp/BWC_ETL_VALIDATIONS.csv  --parallel 3 --silent --output c:/Temp/TestOutput
# python run_validate_scripts.py --conn1 dev/snowflake_dev --conn1 dev/snowflake_dev  --limit 10 --sql C:/Temp/JEFF_VALIDATIONS.csv  --parallel 10  --silent 
# python run_validate_scripts.py --conn1 uat/oracle_etl --conn2 uat/snowflake_uat  --sql C:/Temp/OracleComparison.csv  --parallel 4 --limit 5 --silent 
# python run_validate_scripts.py --conn1 prd/snow_etl  --sql E:/Release_Prod/bwcroot/bwcenv/bwcmisc/BWC_ETL_VALIDATIONS.csv  --parallel 15  --output E:/Release_Prod/ETL_REPORTS --saveto BWC_AUDIT.PUBLIC.BWC_ETL_VALIDATION_RESULTS --reset --silent 
