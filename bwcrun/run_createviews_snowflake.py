import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
from pathlib import Path


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
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup


def fixcols(args,cols):
    ''' 
    TO DO - ADD NUMBERS AS RESERVE WORDS( ADD QUOTES FOR THOSE COLUMNS HAVING NUMBERS)
    '''
    reserve={"SCHEMA","START","STATUS","TABLE"}
    chars=set('!@#$%^&*')

    cleaned_cols=[]
    for col in cols:
        if any((char in chars)for char in col):
            col=f'"{col}"'
        elif col in reserve:
            col=f'"{col}"'
        cleaned_cols.append(col)

    return cleaned_cols

def get_srcviewmap(args,dbcon,src_tables='',db=''):
    '''
    db is a optional arugument because in snowflake the views can be created in another database.
    
    {'ACCOUNT_TYPE': ['ACCT_TYP_CD', 'ACCT_TYP_NM', 'ACCT_TYP_VOID_IND', 'BWC_DW_EXTRACT_TS', 'BWC_DW_LOAD_KEY', 'BWC_DW_LOAD_START_DTTM'],
     'BWCSTAGEDW_APC_INFO': ['APC_AMT', 'APC_BGN_DATE', 'APC_CODE', 'APC_DESC', 'APC_END_DATE', 'APC_STS_IND_CODE', 'APC_STS_IND_DESC', 'BWC_DW_LOAD_START_DTTM', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE'], 
     'BWCSTAGEDW_API_SUM': ['API_RCPT_DATE', 'BWC_DW_LOAD_START_DTTM', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE', 'MCO_ID_NO', 'MCO_NAME', 'NTWRK_ID_NO', 'TOTAL_FAIL_QTY', 'TOTAL_PASS_QTY', 'TOTAL_SBMTD_QTY'], 
     'BWCSTAGEDW_DIAG': ['BWC_DW_LOAD_START_DTTM', 'CARE_HDR_ID_NO', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE', 'ICD_CODE', 'ICD_SQNC_NO', 'INVC_NO', 'PRNCP_ICD_FLAG', 'PRSNT_ADMSN_TEXT']}
    '''
    
    #src_tables=args.srccon.get_objects(args.srcschema,db)


    table_cols_dict={}

    table_cols_dict=dbcon.get_cols(args.srcschema,db=db)
    if src_tables:
        table_cols_dict2={}
        for idx,table_dict in enumerate(src_tables):
            atable=table_dict['table']
            if atable in table_cols_dict:
                table_cols_dict2[atable]=table_cols_dict[atable]
        table_cols_dict=table_cols_dict2

    #return table_cols_dict,view_cols_dict
    return table_cols_dict

def create_nokey_view(args, srcdb, src_table, src_cols_str, tgt_view):
    sql_option='OR REPLACE TABLE'
    # if args.force:
    #     sql_option='OR REPLACE VIEW' 
    

    src_fullyqtable=f'{srcdb}.{args.srcschema}.{src_table}'
    tgt_fullyqview=f'{args.tgtdb}.{args.tgtschema}.{tgt_view}'

    
    sql=f'''
    CREATE  {sql_option}  {tgt_fullyqview} AS (
    with add_id as ( 
        select NULL as BWC_ID,
        {src_cols_str}
        from {src_fullyqtable}
    )
            select {src_cols_str}, BWC_ID from add_id
    )
    '''

    return sql


def create_key_view(args,srcdb,src_table,src_cols_str,tgt_view): 
    '''CREATE  OR REPLACE TABLE  ETL_PRESENT.PCMP.ACTIVITY AS (
    with add_id as ( 
        select ACTV_ID as BWC_ID,
        ACTV_CNTX_ID, ACTV_ID, AUDIT_USER_CREA_DTM, AUDIT_USER_ID_CREA, BWC_DW_EXTRACT_TS, BWC_DW_LOAD_KEY, CNTX_TYP_CD, SUBLOC_TYP_CD
        from ETL_SOURCE.PCMP.ACTIVITY where ACTV_ID in (1331101638,1331282595)
    )

    ,get_latest as (
            select
        ACTV_CNTX_ID, ACTV_ID, AUDIT_USER_CREA_DTM, AUDIT_USER_ID_CREA, BWC_DW_EXTRACT_TS, BWC_DW_LOAD_KEY, CNTX_TYP_CD, SUBLOC_TYP_CD, BWC_ID
                    , row_number() over ( partition by BWC_ID order by BWC_DW_EXTRACT_TS desc ) as BWC_ROWNUM
                                    from add_id
                qualify BWC_ROWNUM =1
            )

    select * from get_latest
    ) 
    '''
    sql_option='OR REPLACE TABLE'
    # if args.force:
    #     sql_option='OR REPLACE VIEW' 
    src_key=args.load_version_table.get('key')

    src_fullyqtable=f'{srcdb}.{args.srcschema}.{src_table}'
    tgt_fullyqview=f'{args.tgtdb}.{args.tgtschema}.{tgt_view}'
    
    sql=f'''
    CREATE  {sql_option}  {tgt_fullyqview} AS (
    with add_id as ( 
        select {src_key} as BWC_ID,
        {src_cols_str}
        from {src_fullyqtable}
    )

    ,get_latest as (
            select
        {src_cols_str} , BWC_ID
                    , row_number() over ( partition by BWC_ID order by BWC_DW_EXTRACT_TS desc ) as BWC_ROWNUM
                                    from add_id
                qualify BWC_ROWNUM = 1
            )

    select {src_cols_str}, BWC_ID from get_latest
    )
    '''
    return sql  

def create_history_view(args,srcdb,src_table,src_cols_str,tgt_view): 
    '''CREATE  OR REPLACE TABLE  ETL_PRESENT.PCMP.ACTIVITY AS (
    with add_id as ( 
        select ACTV_ID as BWC_ID,
        ACTV_CNTX_ID, ACTV_ID, AUDIT_USER_CREA_DTM, AUDIT_USER_ID_CREA, BWC_DW_EXTRACT_TS, BWC_DW_LOAD_KEY, CNTX_TYP_CD, SUBLOC_TYP_CD
        from ETL_PRESENT.PUBLIC.test_history where ACTV_ID in (1331101638,1331282595)
    ),

    get_latest as (
            select
        ACTV_CNTX_ID, ACTV_ID, AUDIT_USER_CREA_DTM, AUDIT_USER_ID_CREA, BWC_DW_EXTRACT_TS, BWC_DW_LOAD_KEY, CNTX_TYP_CD, SUBLOC_TYP_CD, BWC_ID
                    , row_number() over ( partition by ACTV_CNTX_ID, ACTV_ID, AUDIT_USER_CREA_DTM, AUDIT_USER_ID_CREA,CNTX_TYP_CD, SUBLOC_TYP_CD, BWC_ID order by BWC_DW_EXTRACT_TS desc ) as BWC_ROWNUM
                                    from add_id
                qualify BWC_ROWNUM =1
            )
  
    select * from get_latest
    ) 
    '''
    sql_option='OR REPLACE TABLE'
    # if args.force:
    #     sql_option='OR REPLACE VIEW' 
    src_key=args.load_version_table.get('key')

    src_fullyqtable=f'{srcdb}.{args.srcschema}.{src_table}'
    tgt_fullyqview=f'{args.tgtdb}.{args.tgtschema}.{tgt_view}'
    
    bsns_cols = [col.strip() for col in src_cols_str.split(',') if not col.strip().startswith('BWC_')]
    bsns_cols_str=','.join(bsns_cols)
    args.log.info(bsns_cols_str)
    sql=f'''
    CREATE  {sql_option}  {tgt_fullyqview} AS (
    with add_id as ( 
        select {src_key} as BWC_ID,
        {src_cols_str}
        from {src_fullyqtable}
    )

    ,get_latest as (
            select
        {src_cols_str}, BWC_ID
                    , row_number() over ( partition by {bsns_cols_str} order by BWC_DW_EXTRACT_TS desc ) as BWC_ROWNUM
                                    from add_id
                qualify BWC_ROWNUM = 1
            )

    select {src_cols_str}, BWC_ID from get_latest
    )
    '''
    return sql  
    

def create_stream_view(args,srcdb,src_table,src_cols_str,tgt_view):
    sql_option='OR REPLACE VIEW'
    # if args.force:
    #     sql_option='OR REPLACE VIEW' 

    src_fullyqtable=f'{srcdb}.{args.srcschema}.{src_table}'
    tgt_fullyqview=f'{args.tgtdb}.{args.tgtschema}.{tgt_view}'
    src_key=args.load_version_table.get('key')

    sql=f'''
    CREATE  {sql_option}  {tgt_fullyqview} AS (
    with add_id as ( 
        select {src_key} as BWC_ID,
        Z_META_ORACLE_CDC_PRECISIONTIMESTAMP as BWC_DW_EXTRACT_TS,
        Z_META_ORACLE_CDC_OPERATION AS BWC_DW_CDC_OPERATION,
        {src_cols_str}
        from {src_fullyqtable}
    )

    ,get_latest as (
            select
                    {src_cols_str}, BWC_ID
                    , row_number() over ( partition by BWC_ID order by BWC_DW_EXTRACT_TS desc ) as BWC_ROWNUM
                                    from add_id
                qualify BWC_ROWNUM = 1
            )
    , remove_ids as (
                select 
                {src_cols_str}
                    from get_latest
                where BWC_DW_CDC_OPERATION != 'DELETE'
                )

    select {src_cols_str}, BWC_ID from remove_ids
    )
    '''
    return sql  
    

def create_view_sql2(args,srcdb,src_table,src_cols_str,tgt_view):
    
    '''
    
    '''
    src_key=args.load_version_table.get('key')
    src_option=args.load_version_table.get('option')
    if 'Z_META' in src_cols_str.upper():
        sql=create_stream_view(args,srcdb,src_table,src_cols_str,tgt_view)
    elif src_key:
        if src_option == 'history':
            args.log.info(f'Creating history table for {tgt_view}')
            sql=create_history_view(args,srcdb,src_table,src_cols_str,tgt_view)  
        else: 
            sql=create_key_view(args,srcdb,src_table,src_cols_str,tgt_view)
    else:
        sql=create_nokey_view(args,srcdb,src_table,src_cols_str,tgt_view)

    return sql
        

def save_sql(args,table,sql):
    fname=Path(args.tgtdata)/(table+'.sql')
    fname.write_text(sql) 
    args.log.debug(fname)
   
    
def main_parallel(allargs):
    

    '''
    assumes same database and connection is used for src and tgt!
    First check to see if Target view already exists
    If it does, verify nothing has changed then skipped.
    If it does not, create a view.
    '''
    try:
        args,dbcon_dict,src_table,src_table_cols,srcdb,load_version_tables_dict=allargs
        tgt_view=src_table
        args.log=inf.setup_log(args.logdir,app=f'child_{src_table}',prefix='     _')

        if not src_table_cols:
                return f'{src_table}: Could not find columns'
        
        args.dbcon=dblib.DB(dbcon_dict,log=args.log,port=dbcon_dict.get('port',''))
        
        args.log.debug(f'in base create views {src_table} {src_table_cols}')

        args.load_version_table=load_version_tables_dict[src_table]
        #src_key=load_version_table.get('key')
        #src_option=load_version_table.get('option')
        src_cols_str=', '.join(src_table_cols)
        sql=create_view_sql2(args,srcdb,src_table,src_cols_str,tgt_view)

        args.dbcon.exe(sql)
        save_sql(args,tgt_view,sql)
        args.log.debug(f'Done')
        return src_table
    except:
        errormsg=inf.geterr()
        args.log.info(f'Error {src_table}')
        args.log.debug(errormsg)
        return f'{src_table}: {errormsg}'


def main_parallel2( allargs ):
    try: 
        args, dbcon_dict, table_sql = allargs
        table, sql = table_sql
        args.log = inf.setup_log( args.logdir, app = f'child_{table}', prefix = '     _')
        
        args.dbcon = dblib.DB( dbcon_dict, log = args.log, port = dbcon_dict.get('port','') )
        args.dbcon.exe( sql )
        save_sql( args, table, sql )
        args.log.debug( f'Done' )
        return table

    except:
        errormsg = inf.geterr()
        args.log.info( f'Error {table}' )
        args.log.debug( errormsg )
        return f'{table}: {errormsg}'

def main_parallel3( allargs ):
    try:
        args = None
        args, dbcon_dict, src_table, src_table_cols = allargs
        tgt_view = src_table
        args.log = inf.setup_log(args.logdir, app = f'child_{src_table}', prefix = '     _' )
        args.dbcon = dblib.DB( dbcon_dict, log=args.log, port = dbcon_dict.get('port','') )
        
        src_fullyqtable = f'{args.tgtdb}.{args.tgtschema}.{tgt_view}'
        tgtdb = args.tgtdb.upper().replace( '_PRESENT', '_VIEWS' )
        tgt_fullyqview = f'{tgtdb}.{args.tgtschema}.{tgt_view}'
        # src_table_cols=args.dbcon.get_cols(args.tgtschema,table=tgt_view,db=args.tgtdb)
        # if not src_table_cols:
        #         return f'{src_table}: Could not find columns'
        src_cols_str = ', '.join( src_table_cols )
                
        args.log.debug( f'in base create views {src_table} {src_table_cols}' )
        
        sql_option = 'VIEW IF NOT EXISTS' 

        if args.force:
          sql_option = 'OR REPLACE VIEW'
        
             
        sql=f'''
            CREATE  {sql_option}  {tgt_fullyqview} AS (
            
            select {src_cols_str} from {src_fullyqtable}
            )
            '''

        args.dbcon.exe(sql)
        save_sql( args, tgt_view, sql )
        args.log.debug( f'Done' )
        return src_table
    except:
        errormsg = inf.geterr()
        if args:
            args.log.info( f'Error {src_table}' )
            args.log.debug( errormsg )
        else:
            print( f'{src_table}: {errormsg} ')
        return f'{src_table}: {errormsg}'



    
        
def get_load_version_dict(args):
    args.log.info(f'Using load file {args.load_file}')
    source_rows=[]
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['table'].startswith('#'): continue
        source_rows.append(source_dict)

    return source_rows


def make_union_sql(args,src_table_cols_dict,srcdb):
    standard_name = {}
    suffix='_BWC'
    # If the table contains a numeric as a last value, add it to the standard_name dictionary
    for table in src_table_cols_dict:
        if table[-1].isdigit():
            table_name = table.rstrip('0123456789')
            if not table_name.endswith(suffix): continue
            standard_name[table] = {'ALIAS':table_name.rsplit(suffix,1)[0],'NAME':table,'COLS':src_table_cols_dict.get(table)}
    # {'ALIAS': 'ACCOUNT', 'NAME': 'ACCOUNT_BWC1', 'COLS': ['COL1', 'COL2', 'COL3']}

    # Create a unique list of views to be created
    override_list = set()
    for table, values in standard_name.items() :
        override_list.add( values['ALIAS'] )

    # For each view to be created, build the sql
    sqls=[]
    for alias in override_list:
            fq_alias=f'{args.tgtdb}.{args.tgtschema}.{alias}'
            table_list, innersql = [],[]
            sql = f'CREATE OR REPLACE TABLE {fq_alias} as \n(\n'
            table_cols=''
            for output, tables in standard_name.items():
                if tables['ALIAS'] == alias:
                    table_toadd = tables['NAME'] 
                    if not table_cols:
                        table_cols=', '.join(standard_name[table_toadd].get('COLS'))
                                           
                    fq_table_toadd=f'{srcdb}.{args.srcschema}.{table_toadd}'
                    table_list.append( table_toadd )
                    innersql.append(  f"   select {table_cols} from { fq_table_toadd }" )
            sql += f'\nUNION ALL \n'.join( innersql )
            sql += f'\n)'
            sqls.append((alias,sql))
    return sqls

#---------- Standard setup


def process_results(args,results):
    errors=[]
    idx = 0
    for idx,result in enumerate(results):
        if ':' in result:
            errors.append(result.split(':',1)[0])
    args.log.info( f'Out of {idx+1} tables found {len(errors)} errors')
    return errors

def process_args():

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    #datadir = "I:/Data_Lake/CAM"

    parser = argparse.ArgumentParser(
        description='command line args', epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00", add_help=True)
    # required
    parser.add_argument( '--srcdir', required=True, help='src dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument( '--load_version', required=True, help='csv file that defines loading for a schema')
    #boolean
    #parser.add_argument( '--keep_prefix', default=True,action='store_false',help='if there is a table prefix, remove it viewname')
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )
    parser.add_argument( '--force', action='store_true', help='Force creation of views again' )
    # optional
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument('--table_prefix', required=False, default='', help='only process tables with this prefix, auto adds _')
    parser.add_argument( '--snow_srcdb', required=False,default='', help='Snowflake Database source loaded into')
    #parser.add_argument('--test', required=False, default='', help='only process tables with this prefix, auto adds _')
    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')
    

    args = parser.parse_args()
    if args.eldir=='dev': 
        args.eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"

    args.root=Path(__file__).resolve().parent.parent
    args.loaddir=args.root/'bwcloads'
    
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14AM

    if not args.load_key: args.load_key=ymd

    inf.build_args_paths(args,use_load_key=True)

    if args.table_prefix:
        args.table_prefix=args.table_prefix.upper()+'_'

    args.load_file=args.loaddir/args.srcenv/args.srcdb/f'{args.srcschema}_{args.load_version}.csv'

    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

    

def main():
    '''
    To Do: 
        - Support Column name remapping
        - Add preserve Order column (ordered dict)


    Namespace(cleanlog=False, eldir=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL'),
    keep_prefix=True, log=<Logger parent (DEBUG)>, 
    logdir=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devv/DEV_VIEWS/PH__PCMP/logs'),
    srcdata=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devt/DEV_SOURCE/PH__PCMP/data'), 
    srcdb='DEV_SOURCE',
    srcdb_dict={'server': 'dza60922.us-east-2.aws', 
      'db': 'DEV_SOURCE', 'login': 'X10140138', 
    , 'type': 'snowflake', 'warehouse': 'WH_BI'}, 
    tgtdir=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devv/DEV_VIEWS/PH__PCMP'),
    tgtenv='dev', 
    tgtkey='snowflake_devv', 
    tgtlog=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devv/DEV_VIEWS/PH__PCMP/logs'),
    tgtschema='PH__PCMP')
    


    '''
    try:
        args = None
        args = process_args()
        #Only needed for Snowflake
        srcdb_dict = dbsetup.Envs[args.srcenv][args.srckey]
        tgtdb_dict = dbsetup.Envs[args.tgtenv][args.tgtkey]
    
        
        dbcon = dblib.DB( tgtdb_dict, log = args.log, port = tgtdb_dict.get('port','') )
        '''CREATE SCHEMA  IF NOT EXISTS UAT_SOURCE.dbmoit00;'''
        sql = f'CREATE SCHEMA IF NOT EXISTS {args.tgtdb.upper()}.{args.tgtschema.upper()}'
        dbcon.exe(sql)

        srcdb = args.srcdb
        if args.snow_srcdb:
            srcdb = args.snow_srcdb    
        #src_table_cols_dict,src_view_cols_dict=get_srcviewmap(args,db=args.srcdb)
        load_version_tables = get_load_version_dict(args)
        load_version_tables_dict={}
        for row in load_version_tables:
            load_version_tables_dict[row['table'].upper()] = row
        src_table_cols_dict = get_srcviewmap(args,dbcon,load_version_tables,db=srcdb)
        src_table_cols_dict_union=get_srcviewmap(args,dbcon,db=srcdb)


        # Parallel Processing
        
        all_args=[];errors=[]
        for src_table,src_table_cols in src_table_cols_dict.items():
            if not src_table_cols:
                errors.append(f'{src_table}: Could not find columns')
                continue
            table_args=args,tgtdb_dict,src_table,src_table_cols,srcdb,load_version_tables_dict
            all_args.append(table_args)
        args.log.info(f'Found Missing {len(errors)} tables')
        args.log.info(f'Processing {len(all_args)} in parallel')
        #create_views(args,src_table_cols_dict,srcdb,load_version_tables_dict)
        results=inf.run_parallel(main_parallel,all_args,parallel=15)
        errors+=process_results(args,results)
        if errors:
            raise ValueError(f'Bad Tables {errors}')
        #parallel processing for unions
        all_args=[];errors=[]
        sqls = make_union_sql(args,src_table_cols_dict_union,srcdb)
        if sqls: 
            for sql in sqls:
                table_args=[args,tgtdb_dict,sql]
                all_args.append(table_args)
            results=inf.run_parallel(main_parallel2,all_args,parallel=15)
            errors+=process_results(args,results)
            if errors:
                raise ValueError(f'Bad Tables {errors}')
        
        # parallel create VIEWS
        present_table_cols_dict=dbcon.get_cols(args.tgtschema,db=args.tgtdb)
        all_args=[];errors=[]
        for present_table,present_table_cols in present_table_cols_dict.items():
            if not present_table_cols:
                errors.append(f'{present_table}: Could not find columns')
                continue
            #table_args=args,tgtdb_dict,present_table,present_table_cols,srcdb,load_version_tables_dict
            table_args=args,tgtdb_dict,present_table,present_table_cols
            all_args.append(table_args)
        args.log.info(f'Found Missing {len(errors)} tables')
        args.log.info(f'Processing {len(all_args)} in parallel')
        # code to make sure _VIEWS schema exists
        tgtdb=args.tgtdb.upper().replace('_PRESENT','_VIEWS')
        v_sql = f'CREATE SCHEMA IF NOT EXISTS {tgtdb}.{args.tgtschema}'
        dbcon.exe(v_sql)
        
        results=inf.run_parallel(main_parallel3,all_args,parallel=15)
        errors+=process_results(args,results)
        if errors:
            raise ValueError(f'Bad Tables {errors}')
        

        args.log.info('--==DONE==--')
            
    except:
        if args:
            args.log.info(inf.geterr())
            raise 
        else:
            print(inf.geterr())
            raise



if __name__=='__main__':
    main()


'''
Latest Example:
python run_createviews_snowflake.py --srcdir /uat/snow_etl/pub1/PCMP --tgtdir /uat/snow_etl/UAT_PRESENT/PCMP  --load_version other --snow_srcdb UAT_SOURCE --eldir dev

*** Query for Reference ***

with add_id as ( 
    select PLCY_INVC_ID as BWC_ID,Z_META_ORACLE_CDC_PRECISIONTIMESTAMP as BWC_DW_EXTRACT_TS,*
    from pda1.pcmp.bwc_policy_invoice
)
, get_latest as (
   select
        *,
        row_number() over ( partition by BWC_ID order by BWC_DW_EXTRACT_TS desc ) as BWC_ROWNUM,
        count(*) over ( partition by BWC_ID order by BWC_DW_EXTRACT_TS desc ) as BWC_ID_COUNT
    from add_id
    qualify BWC_ROWNUM = 1
)
, remove_ids as (
    select *
        from get_latest
     where z_meta_oracle_cdc_operation != 'DELETE'
)

select * from remove_ids;

'''
