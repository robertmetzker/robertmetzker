import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime,shutil,openpyxl,xlrd
from pathlib import Path
from collections import OrderedDict

def set_libpath():
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

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

import threading, queue


def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    eldir=f"I:/Data_Lake/Kronos/"
 
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)

    #req
    parser.add_argument( '--tgtdir', required=True,help='data target /env/db/schema')
    #boolean

    #optional

    #--choices

    #--standard

    #parser.add_argument( '--dwpath', default='//mswg9/groups/IT/ETL/TRANSFER/PD',help='bwcstd or custom header')
    parser.add_argument( '--eldir', default=eldir,help='default directory to use')
    parser.add_argument( '--cleanlog',action='store_true', help='changes the logger to overwrite mode')
    
    args = parser.parse_args()
    args.rerun=True

    #--- load_key
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    args.load_key=ymd
    args.load_ts=ymd_hms

    #--- default paths and load file
    tgtdir=args.tgtdir.upper()

    args.tgtdir=args.tgtdir.strip('/')

    ##### Need to set up an etl folder for etl setup
    args.tgt_env, args.tgt_key = args.tgtdir.split('/', 1 )
    args.tgtdb = 'ETL_ODS'
    args.tgt_schema = 'KRONOS' 

    args.eldir = Path(args.eldir)
    args.logdir=args.eldir/'logs'

    args.root=Path(__file__).resolve().parent.parent

    # -------   setup standard STAGING->SOURCE->VIEWS framework

    args.logdir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app='parent',cleanlog=args.cleanlog)
    args.log.debug('Running on:'+os.environ['COMPUTERNAME'])
    #args.log.info(f'{dest_final}')
    args.log.info(f'processing from:{args.tgtdir}')
    args.stagedir = '/RPD1/PUBLIC/BWC_ETL_VALIDATION/'
    args.snow_prefix='@~'

    return args


def find_files_for_load(args):
    found_files = []
    path = args.eldir
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith('.xls') or file.endswith('.xlsx'):
                foundfile = os.path.join(root,file)
                if '\\archived' in foundfile:
                    print(f'<<< archived file being skipped = {foundfile}>>>')
                else:
                    found_files.append(foundfile)
    return found_files


def convert_file_to_csv(args, workbook):
    #workbook='C:/Temp/oracle hours data.xlsx'
    if workbook.endswith('.xlsx'):
        csvfile=workbook.replace('.xlsx','.csv')
        csvfile = csvfile.replace( ' ', '_' )
        if os.path.exists( csvfile ): 
            pass
        else:
            wb=openpyxl.load_workbook(workbook)
            ws=wb.active
            with open(csvfile, 'w', newline='') as f:
                c = csv.writer(f)
                for row in ws.rows:
                    c.writerow([cell.value for cell in row])
    elif workbook.endswith('.xls'):
        csvfile=workbook.replace('.xls','.csv')
        csvfile = csvfile.replace( ' ', '_' )
        if os.path.exists( csvfile ): 
            pass
        else:
            wb=xlrd.open_workbook(workbook)
            ws=wb.sheet_by_index(0)
            with open(csvfile, 'w', newline='') as f:
                c = csv.writer(f)
                for row in range(ws.nrows):
                    c.writerow(ws.row_values(row))
    else:
        return 

    print(workbook,' => ' , csvfile)
    return csvfile


def snow_put(args,  comparison_file, stagedir=''):
    '''
        requires the full path
    put file://C:\temp\PCMP--uat_snowflake_vs_uat_oracle_comparison_20220819.csv @~/DEV_EDW/PUBLIC/SCHEMA_COMPARE/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';

    '''
    db1_conn = get_dbcon(args, args.tgt_env, args.tgt_key)
    
    stagedir = args.stagedir

    # Clean out staging before uploading new results
    remove_sql = f'rm @~{stagedir}'
    print(remove_sql)
    rm_result = db1_conn.exe(remove_sql)
    if rm_result:
        print( f'## REMOVED FILES FROM STAGE...' )

    stage_cmd=f'''put file://{comparison_file} {args.snow_prefix}{stagedir} auto_compress=true;'''
    print( stage_cmd )
    result = db1_conn.exe( stage_cmd )
    staged_files = snow_list_stage( args, db1_conn, stagedir )
    
    #if not staged_files:
        #raise Warning(f'Missing staged file: {path}')
        #args.log.info(f'Staged {staged_files}')

    print(f'\n-- Staged {staged_files}')
    return staged_files


def get_dbcon(args, env, db):
    '''
    Returns a database connection object.  contained in object:
    {'server': 'XDW18VRPD01.bwcad.ad.bwc.state.oh.us', 'db': 'RPD1', 'login': 'xxxx', 'passwd': 'zzzzzz', 'type': 'vertica'}
    '''

    tgtdb = dbsetup.Envs[env][db]
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con = dblib.DB(tgtdb, log='', port = tgtdb.get('port',''))
    return con


def snow_list_stage(args, db1_conn, stagedir=''):
    '''
        https://docs.snowflake.com/en/sql-reference/sql/list.html

    list @~/RPD1/PUBLIC/BWC_ETL_VALIDATION/;
    [{'name': 'RPD1/PUBLIC/BWC_ETL_VALIDATION--uat_snowflake_vs_uat_oracle_comparison_20220819.csv.gz', 'size': 688, 'md5': 'f500ff108c48d4339ffab8a8f070fb9e', 'last_modified': 'Fri, 19 Aug 2022 18:01:57 GMT'}]   
    '''
    stagedir = args.stagedir
    sql=f'list {args.snow_prefix}/{stagedir}; '

    stage_files=[ row for  row in db1_conn.fetchdict(sql) ]
    return stage_files


def snow_copy_into(args, stagedir, comparison_file ):
    '''
    if the copy into doesn't work, we may need to start using force=TRUE to force it to reload.  If the results of the run are the same 
    as the previous execution, snowflake will assume it has already processed the file and not load it unless forced.'''
    
    db1_conn = get_dbcon(args, args.tgt_env, args.tgt_key)
    file_format = f"""file_format =  (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"' )  """
    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""
    if 'LEGACY' in comparison_file:
        file_fields = ['NAME', 'ENUMBER', 'TMKP_DATE', 'TIME_WORKED']
        saveto='ETL_ODS.KRONOS.LEGACY_TMKP'
        file_to_load = os.path.basename(comparison_file)
    else:
        file_fields = ['NAME', 'ENUMBER', 'TMKP_DATE', 'LBR_ACCT', 'PAY_CODE', 'TIME_WORKED']
        saveto='ETL_ODS.KRONOS.KRONOS_TMKP'
        file_to_load = os.path.basename(comparison_file)

        
    fields = ', '.join( file_fields )
    selectstmt= ''
    stmt = []

    for x in range( len(file_fields) ):
        stmt.append( f's.${x+1}')
    selectstmt = str( ', '.join(stmt))

    stagedir = args.stagedir
    # copy_cmd=f"""copy into {dbname}.{schema}.{table} from {args.snow_prefix}/{stagedir} {file_format} on_error='continue'; """
    copy_cmd = f"""copy into {saveto} ( {fields} ) from (select {selectstmt} from {args.snow_prefix}{stagedir}{file_to_load} s) {file_format} TRUNCATECOLUMNS= TRUE on_error='continue'; """
    # args.log.info(copy_cmd)
    print(f' --COPYING via {copy_cmd}' )
    result=list( db1_conn.exe(copy_cmd) )
    # args.log.info(str(result))
    print( str(result) )
    return result[0]


def main():
    found_files, csv_files = [], []
    args=process_args()
    found_files = find_files_for_load(args)
    #print(found_files)

    for file in found_files:
        csvfile = convert_file_to_csv(args, file)
        csv_files.append(csvfile)
    #print(csv_files)

    for file in csv_files:
        print(f'### LOADING RESULTS TO SNOWFLAKE ###')
        snow_put(args, file, args.stagedir)
        snow_copy_into( args, args.stagedir, file )
        os.remove(file)
    
    for file in found_files:
        # I:\Data_Lake\Kronos\archived\KRONOS_TMKP
        archive_path = file.replace('\\Kronos', '\\Kronos\\archived', 1)
        os.rename(file, archive_path)
    
    print(f'### Kronos Load Complete ###')


if __name__=='__main__':
    main()

# python run_kronos_load.py --tgtdir /etl/snow_etl