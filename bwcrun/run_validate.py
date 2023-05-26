import sys, os, argparse, openpyxl, re
from tqdm import tqdm
from pathlib import Path
from openpyxl import Workbook
from sheet2dict import Worksheet
from datetime import datetime
'''
re is used to correct the names of the tests for logging
datetime is used for format the filename
'''


def set_libpath():
    r'''
    Set path import to be relative to the location of the dir the prog is run from
    assuming you are in ..\bwcroot\bwcenv\bwcrun\ 
    becomes:  ..\bwcroot\
    by using parent of parent (i.e.  cd ../..)
    '''
    import sys,os
    from pathlib import Path
    prog_path=Path(os.path.abspath(__file__))
    libpath=str(prog_path.parent.parent.parent)
    sys.path.append(libpath)
    # print('using libpath',libpath)

set_libpath()
from bwcsetup import dbsetup
from bwcenv.bwclib import inf,dblib


def process_args():
    etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python validate.py --srcdir RDA1/bwc_etl",add_help=True)

    #required
    parser.add_argument('-e','--env', required=True, help='Please set env variable')
    parser.add_argument('--sf', required=False, help='Please enter a Snowflake connection to use')
    parser.add_argument('--vt', required=True, help='Please enter a Vertica connection to use')
    
    #optional
    parser.add_argument('--sql', required=False, help='default directory for all logs, data files')
    parser.add_argument('--eldir', required=False, default=etldir,help='default directory for all extracts')
    parser.add_argument('-s','--silent', required=False, default=False, action='store_true', help='include if you want to silence validation query results')
    parser.add_argument('-l','--limit', required=False, default=False,  help='include if you want to limit the number of DW_REPORT queries as a test')
    parser.add_argument('-p','--parallel', required=False, default=20, help='number of instances to spool up.')
    
    args = parser.parse_args()

    # args.parallel = 20
    args.parallel = int( args.parallel )

    args.etldir=Path(args.eldir)
    args.etldir=Path(etldir)
    if not args.silent:
        args.logdir=args.etldir/'logs'
        args.log=inf.setup_log(args.logdir,app='parent')
        args.log.info(f'processing in {args.eldir}')
        print( f'\t=== Using ETLDIR: {etldir}' )
        print( f'--Processing in: {args.eldir}' )

    return args


def get_dbcon(args, env, db):
    tgtdb = dbsetup.Envs[env][db]
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con=dblib.DB(tgtdb,log='',port=tgtdb.get('port',''))
    return con


def get_val_queries(args):
    """Validation Queries were in Vertica_DEV, but we replicate from Vertica Prod_Weekly.
    The queries also exist in Snowflake, but not in Vertica Prod or Prod_Weekly."""
    get_queries_vert = f'SELECT * from BWC_ETL.VALIDATION5 '
    get_queries_snow = f'SELECT * from RDA1_SOURCE.BWC_ETL.VALIDATION5 '
    if args.limit:
        get_queries_vert += f" where lower(VAL_SQL) not like '%from %_act.%' limit {args.limit} "
        get_queries_snow += f" where lower(VAL_SQL) not like '%from %_act.%' limit {args.limit} "
    row_gen = []

    print(f'::: Fetching Validation Queries :::')
    # EXAMPLE parsed:  get_dbcon( args, dev, vertica_dev)
    # Change between sfcon and vtcon to change the source of the Validation queries...
    sfcon = get_dbcon(args, args.env, args.sf )
    row_gen = sfcon.fetchdict(get_queries_snow)

    # vtcon = get_dbcon(args, args.env, args.vt )
    # row_gen = vtcon.fetchdict(get_queries_vert)
    # print(vtcon.fetchdict(get_queries_vert))

    query_list =[]
    for idx,row in enumerate(row_gen):
        # {'CNTRL_TOTAL_ID': 10003, 'QUERY_TYPE': 'DW_REPORT', 'VAL_SQL': 'select count(DISTINCT CLM_AGRE_ID) from (select CLM_AGRE_ID, INDST_CD_EFF_DT from DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY group by 1,2 having count(*)>1)x'}
        row_sql = {}
        row_sql['CNTRL_TOTAL_ID'] = row['CNTRL_TOTAL_ID']
        row_sql['QUERY_TYPE'] = row['QUERY_TYPE']
        row_sql['CNTRL_TOTAL_SUB_NAME'] = row['CNTRL_TOTAL_SUB_NAME']
        row_sql['VT_RESULTS'] = ''
        row_sql['SF_RESULTS'] = ''
        row_sql['VAL_SQL'] = row['VAL_SQL']
        query_list.append( row_sql.copy() )
    for row in enumerate(row_gen):
        print(row)
    
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
    problem_statements = [ 'FROM ','AND ','UNION','MINUS','JOIN ','INNER ','OUTER ','LEFT ','RIGHT ','SELECT','WHERE ','HAVING ', 'GROUP ' ]

    for problem in problem_statements:
        if problem.lower() in qsql.lower():
            problem_LC = problem.lower()
            problem_UC = problem.upper()
            qsql = re.sub(rf'{problem_LC}|{problem_UC}',rf' {problem_UC} ', qsql )

    return qsql

def run_sql(all_args):
    args,sql=all_args

    qauditid = sql['CNTRL_TOTAL_ID']
    qtype = sql['QUERY_TYPE']
    qcontrol = sql['CNTRL_TOTAL_SUB_NAME']
    qcleanname = re.sub('[^0-9a-zA-Z ]+','',qcontrol)

    qsql = clean_sql(sql['VAL_SQL'])
    sf_sql = convert_sf_sql( qsql )
    
    if not args.silent:
        if '.' in sql:
            logname=sql.split('.')[-1].split()[0]
        logname = f'{qtype}_{qauditid}_{qcleanname}'
        args.log=inf.setup_log(args.logdir,app=f'child_{logname}')
        args.log.info(f'processing in {args.etldir}')


    if not args.silent:
        args.log.info(f'--- SQL:\n{qsql}')
        
    # Connect to Vertica and Run Queries
    vtcon = get_dbcon(args,args.env,args.vt )
    if not args.silent:
        args.log.info('VERTICA <== in run_sql')
    row_gen = vtcon.fetchdict(qsql)
    
    for i in tqdm (range(1), desc=f"Executing VERTICA SQL ({qauditid})..." ):
        for idx,row in enumerate(row_gen):
            # TODO: Handle results which are more than a single column--
            # Noise here due to the keys coming back in various forms...
            try:
                result = list(row.values())[0]
            except Exception as e:
                if not args.silent: args.log.error(f'RESULTS: {e}')
            if result == None:
                result = '~'
            sql['VT_RESULTS'] = result
            if not args.silent: args.log.info(f'RESULTS: {result}')

            if idx>2: break
            if not args.silent:
                print(sql)

    if args.sf:
        # Connect to Snowflake and Run Queries
        sfcon = get_dbcon(args,args.env,args.sf )
        if not args.silent:
            args.log.info('SNOWFLAKE <== in run_sql')
        row_gen = sfcon.fetchdict(sf_sql)

        for i in tqdm (range(1), desc=f"Executing SNOWFLAKE SQL ({qauditid})..." ):
            for idx,row in enumerate(row_gen):
                # TODO: Handle results which are more than a single column--
                # Noise here due to the keys coming back in various forms...
                result = list(row.values())[0]
                if result == None:
                    result = '~'
                sql['SF_RESULTS'] = result
                if not args.silent: args.log.info(f'RESULTS: {result}')

                if idx>2: break
                if not args.silent:
                    print(sql)
                
    return sql


def saveresults( args, results ):
    drctry = Path('C:/Temp')
    now = datetime.now()
    datestring = now.strftime("%Y%m%d")

    validation_file = f'validation_results_{datestring}.xlsx'
    
    if not args.silent:
        args.log.info(f'Saving the output to {drctry}/{validation_file}')
    else:
        print(f'\n\nSaving the output to {drctry}/{validation_file}')

    # Output the results of the audit queries to a worksheet via openpyxl
    wb = Workbook()
    ws = wb.active

    # Write Headers
    headers = ['CNTRL_TOTAL_ID','QUERY_TYPE','CNTRL_TOTAL_SUB_NAME','VT_RESULTS','SF_RESULTS','VAL_SQL']
    ws.append( headers )

    # Write Data
    for i in tqdm (range(1), desc="Saving..."):
        for row in results:
            row['VAL_SQL'] = row['VAL_SQL'].replace('\x1a','')
            output = list( row.values() )
            
            # TODO:  Cleanup of VAL_SQL for 60053/60054 with invalid characters...
            try:
                ws.append( output )
            except:
                row['VAL_SQL'] = '-- See Source for Actual SQL ---'
                ws.append( output )

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
    all_args=[( args,sql) for sql in query_list]
    
    if not args.silent:
        logging = True
    else:
        logging = False

    print(f'  -- Spawning {args.parallel} processes --')

    results = inf.run_parallel(run_sql,args=all_args,parallel=args.parallel, log = logging)    

    # for qry in tqdm( query_list, total=qrycnt):
    #     results = inf.run_parallel(run_sql,args=all_args,parallel=args.parallel, log = logging)

    saveresults( args, results )

    if not args.silent:
        args.log.info(f'### DONE ###')
    else:
        print(f'### DONE ###')
    
if __name__ == '__main__':
    main() 



# cd bwcroot\bwcenv\bwcrun
# python run_validate.py --env dev --vt vertica_dev --limit 10 --silent
# python run_validate.py --env dev --vt vertica_dev --sf snowflake_dev  --limit 30 --silent
# python run_validate.py --env prd --vt vertica_testprd --sf snowflake_prd

# TODO-  Grab from SF first and then compare to prod_weekly in vertica...
# python run_validate.py --env prd --vt vertica_me_w --sf snowflake_prd  --limit 30 --silent
# python run_validate.py --env prd --vt vertica_testprd --sf snowflake_prd  --limit 3000 --silent --parallel 3