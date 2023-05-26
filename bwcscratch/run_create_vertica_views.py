#std libs
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
    args,sql1_2=all_args

    view,sql1,sql2=sql1_2

    logname=view
    args.log=inf.setup_log(args.logdir,app=f'child_{logname}')
    #args.log.info(f'processing in {args.etldir}')

    tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))

    args.log.info('in main_parallel')
    row_gen1=f'no table found {view}'
    if sql1: 
            row_gen1 = list(tgtcon.fetchdict(sql1))[0]

    row_gen2 = list(tgtcon.fetchdict(sql2))[0]

    return view,row_gen1,row_gen2

def process_results(args,results):
    for result in results:
        print(result)

def build_tgtschema_views(args,views,tgt_tables):

    sql_option='OR REPLACE VIEW'
    # if args.force:
    #     sql_option='OR REPLACE VIEW' 


    sqls=[]
    for view in views:

        src_fullyqtable=f'{args.srcdb}.{args.srcschema}.{view}'
        tgt_fullyqview=f'{args.tgtdb}.{args.tgtschema}.{view}'

        sql1=''
        if view in tgt_tables:

            sql1=f'''
                    DROP TABLE if exists {tgt_fullyqview}
            '''

        sql2=f'''
        CREATE  {sql_option}  {tgt_fullyqview} AS (

                select * from  {src_fullyqtable}
        )
        '''

        sqls.append( [view,sql1,sql2] )

    return sqls



def process_args():
    '''
    '''

    #etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"


    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--srcdir', required=True, help='tgt dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
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
        src_views=tgtcon.get_objects(args.srcschema,db=args.srcdb)
        tgt_tables=tgtcon.get_tables(args.tgtschema,db=args.tgtdb,only_tables=True)

        sqls=build_tgtschema_views(args,src_views,tgt_tables)
        #sqls=sqls[:20]

        all_args=[(args,sql) for sql in sqls]
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
PRD_VIEWS.PCMP ->   RPD1.PCMP8
--tgtdir /env/cred/db/schema
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/PCMP --tgtdir /uat/snow_etl/RUB1/PCMP8 --eldir c:/temp
==========UAT==========
--BWCODS
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DBDWQP00 --tgtdir /uat/snow_etl/RUB1/BWCODS --eldir c:/temp
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DBDWWT00 --tgtdir /uat/snow_etl/RUB1/BWCODS --eldir c:/temp
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DBPYWT00 --tgtdir /uat/snow_etl/RUB1/BWCODS --eldir c:/temp

--BWCRNP
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DBEABP00 --tgtdir /uat/snow_etl/RUB1/BWCRNP --eldir c:/temp

--BWC_PEACH
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DBMOIT00 --tgtdir /uat/snow_etl/RUB1/BWC_PEACH --eldir c:/temp

--PCMP
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/PCMP --tgtdir /uat/snow_etl/RUB1/PCMP8 --eldir c:/temp

--SUBRO
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/BWCCMN --tgtdir /uat/snow_etl/RUB1/BWCSUBRO --eldir c:/temp

--FMSDM_STG
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DBMOIT00 --tgtdir /uat/snow_etl/RUB1/FMSDM_STG --eldir c:/temp

--RISK_CONTROL
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DW_EXTERNAL_RISK_CONTROL --tgtdir /uat/snow_etl/RUB1/DW_EXTERNAL_RISK_CONTROL --eldir c:/temp

--ACTUARIAL
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DW_ACTUARIAL --tgtdir /uat/snow_etl/RUB1/DW_ACTUARIAL --eldir c:/temp

--BWCMISC - TINDX, TBATCH, IC HEARING NOTICES, TDDMDAS and TDDMRMS - Use Vertica for initial load and then files
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/BWCMISC --tgtdir /uat/snow_etl/RUB1/BWCMISC --eldir c:/temp

--SAFETY COUNCIL
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/DW_SAFETY_COUNCIL --tgtdir /uat/snow_etl/RUB1/DW_SAFETY_COUNCIL --eldir c:/temp

--BWC_ACT - Use Vertica for initial load and then files
python run_create_vertica_views.py --srcdir /uat/snow_etl/UAT_VIEWS/BWC_ACT --tgtdir /uat/snow_etl/RUB1/BWC_ACT --eldir c:/temp

==========PRD==========
--BWCODS
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DBDWQP00 --tgtdir /prd/snow_etl/RPD1/BWCODS --eldir c:/temp
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DBPYQP00 --tgtdir /prd/snow_etl/RPD1/BWCODS --eldir c:/temp

--BWCRNP
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DBEABP00 --tgtdir /prd/snow_etl/RPD1/BWCRNP --eldir c:/temp

--BWC_PEACH
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DBMOBP00 --tgtdir /prd/snow_etl/RPD1/BWC_PEACH --eldir c:/temp

--PCMP
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/PCMP --tgtdir /prd/snow_etl/RPD1/PCMP8 --eldir c:/temp

--SUBRO
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/BWCCMN --tgtdir /prd/snow_etl/RPD1/BWCSUBRO --eldir c:/temp

--FMSDM_STG
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DBMOBP00 --tgtdir /prd/snow_etl/RPD1/FMSDM_STG --eldir c:/temp

--RISK_CONTROL
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DW_EXTERNAL_RISK_CONTROL --tgtdir /prd/snow_etl/RPD1/DW_EXTERNAL_RISK_CONTROL --eldir c:/temp

--ACTUARIAL
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DW_ACTUARIAL --tgtdir /prd/snow_etl/RPD1/DW_ACTUARIAL --eldir c:/temp

--BWCMISC - TINDX, TBATCH, IC HEARING NOTICES, TDDMDAS and TDDMRMS - Use Vertica for initial load and then files
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/BWCMISC --tgtdir /prd/snow_etl/RPD1/BWCMISC --eldir c:/temp

--SAFETY COUNCIL
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/DW_SAFETY_COUNCIL --tgtdir /prd/snow_etl/RPD1/DW_SAFETY_COUNCIL --eldir c:/temp

--BWC_ACT - Use Vertica for initial load and then files
python run_create_vertica_views.py --srcdir /prd/snow_etl/PRD_VIEWS/BWC_ACT --tgtdir /prd/snow_etl/RPD1/BWC_ACT --eldir c:/temp

'''
