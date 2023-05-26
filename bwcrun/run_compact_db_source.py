#std libs
from sre_constants import SUCCESS
import sys,argparse,os
from pathlib import Path

#other libs

#local libs
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

def main_parallel(all_args):
    args,srctable,table_sqls=all_args
    schema,table = srctable.split('.')

    logname=table
    args.log=inf.setup_log(args.logdir,app=f'child_{logname}')
    #args.log.info(f'processing in {args.etldir}')

    tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))

    count_source_sql=f'select count(*) as CNT from {args.env}_source.{schema}.{table}'
    count_present_sql=f'select count(*) as CNT from {args.env}_present.{schema}.{table}'

    count_source_before=list(tgtcon.fetchdict(count_source_sql))[0]['CNT']
    count_present_before=list(tgtcon.fetchdict(count_present_sql))[0]['CNT']

    for sql in table_sqls:
        result=list(tgtcon.fetchdict(sql))[0]['status']
        if 'successfully' not in result:
            raise ValueError(f'bad sql {sql}')
 
    count_source_after=list(tgtcon.fetchdict(count_source_sql))[0]['CNT']

    args.log.info('in run_sql')
    #row_gen = tgtcon.fetchdict(sql)

    # count_source=f'select count(*) from {args.env}_source.public.{table}'
    # count_present=f'select count(*) from {args.env}_present.{args.tgtschema}.{table}'

    args.log.info(f'table:{table}, source_count_before:{count_source_before}, present_count_before:{count_present_before}, source_count_after: {count_source_after}')

    return {'table':table, 'source_count_before':count_source_before, 'present_count_before':count_present_before, 'source_count_after': count_source_after,}

def process_results(args,results):
    '''
    present_count_before should match source_count_after.  Shown below:
    {'table': 'CALCULATION_RESULT_ATTR_GRP', 'source_count_before': 44406020, 'present_count_before': 44403617, 'source_count_after': 44403617}
    '''
    errors=[]
    for result in results:
        args.log.info(result)
        if result['present_count_before'] != result['source_count_after']:
            errors.append(result)

    if errors:
        raise ValueError(f'list of counts, that did not match, {errors}')

def build_compact_sql_per_table(args, srctable):
    #sql1=f'create or replace table {args.env}_source.public.{table} clone {args.env}_present.{args.tgtschema}.{table}'
    schema,table = srctable.split('.')
    sql1=f'create or replace table {args.env}_source.public.{table} clone {args.env}_present.{schema}.{table}'
    sql2=f'alter table {args.env}_source.public.{table} drop column bwc_id'
    sql3=f'alter table {args.env}_source.{schema}.{table} swap with {args.env}_source.public.{table}'
    sqls=[sql1, sql2, sql3]
    return sqls

def make_compact_sqls(args, incremental_tables):
    sqls={}
    for table_dict in incremental_tables:
        table = table_dict['TBL']
        table_sqls = build_compact_sql_per_table(args, table)
        sqls[table]=table_sqls
    return sqls


def get_incremental_tables(args):
    '''
    with vw as ( select  *
    from  PRD_EDW.information_schema.views where upper(VIEW_DEFINITION) like '% AS BWC_ID%')
    select distinct 'COMMENT ON column PRD_PRESENT'||'.'||table_schema||'.'||table_name|| '.BWC_ID is ''INCREMENTAL''; '
    from vw;

    select table_schema||'.'||table_name as tbl
     from  prd_present.information_schema.columns 
    where  column_name = 'BWC_ID' 
      and  comment = 'INCREMENTAL';
    '''
    list_sql = f'''
    select table_schema||'.'||table_name as tbl
     from  {args.tgtdb}.information_schema.columns 
    where  column_name  = 'BWC_ID' 
      and  comment      = 'INCREMENTAL'
    '''

    table_list=[]
    tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))
    table_list = tgtcon.fetchall(list_sql)
    # Example of what is returned above:
    # [{'TBL': 'DBEABP00.TEAMDSD'}, {'TBL': 'DBEABP00.TEAONLC'}]

    return table_list

def process_args():
    '''
    '''

    eldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    #eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"


    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    #parser.add_argument('--env',required=True, help='The environment to use')
    #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    #optional
    parser.add_argument('--eldir', required=False, default=eldir,help='default directory for all logs, data files')
    parser.add_argument('--parallel', required=False, default=4,help='number of parallel processes')
    #
    args= parser.parse_args()
    args.parallel=int(args.parallel)

    args.root=Path(__file__).resolve().parent.parent
    #args.loaddir=args.root/'bwcpresent'

    args.tgtdir=args.tgtdir.strip('/')
    if args.tgtdir.count('/') == 3:
        args.tgtenv, args.tgtkey, args.tgtdb, args.tgtschema = args.tgtdir.split('/')
    else:
        print(f'### ERROR: tgtdir needs to match the pattern /env/key/db/schema ###')
        quit()
    
    args.env = args.tgtenv

    inf.build_args_paths(args)
    
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args


def main():
    try:
        args=None
        args=process_args()

        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
        tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))
        
        # TODO: Search for "INCREMENTAL" in information_schema.tables comment column
        incremental_tables = get_incremental_tables(args)

        sqls= make_compact_sqls(args, incremental_tables)

        args.log.info(sqls)           

        all_args=[(args,table,sql) for table,sql in sqls.items()]
        if args.parallel==1: 
            args.log.debug('Running in single threaded mode')
            results= [ main_parallel(allarg) for allarg in all_args ]
        else:
            results=inf.run_parallel(main_parallel,args=all_args,parallel=args.parallel,log=args.log)

        process_results(args,results)

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
--tgtdir /env/cred/db/schema
python run_compact_db_source.py --tgtdir /etl/snow_etl/etl_present/pcmp --eldir c:/temp
python run_compact_db_source.py --tgtdir /dev/snow_etl/dev_present/pcmp --eldir c:/temp
python run_compact_db_source.py --tgtdir /prd/snow_etl/prd_present/pcmp --eldir c:/temp

create or replace table uat_source.public.bwc_policy_status_hist clone UAT_PRESENT.PCMP.BWC_POLICY_STATUS_HIST;

alter table uat_source.public.bwc_policy_status_hist drop column bwc_id; 

select * from uat_source.public.bwc_policy_status_hist;

select *, row_number() over ( partition by HIST_ID order by BWC_DW_EXTRACT_TS desc ) as row_num from UAT_SOURCE.PUBLIC.BWC_POLICY_STATUS_HIST qualify row_num >1;
    
alter table UAT_SOURCE.PCMP.BWC_POLICY_STATUS_HIST swap with UAT_SOURCE.PUBLIC.BWC_POLICY_STATUS_HIST;

with nullchk as 
    (select bwc_id from UAT_PRESENT.PCMP.account_status_type fetch first 1 row only)
select * from nullchk     

'''
