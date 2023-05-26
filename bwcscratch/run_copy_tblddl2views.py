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
    args,sql=all_args

    logname=sql.split('.')[-1].split()[0]
    args.log=inf.setup_log(args.logdir,app=f'child_{logname}')
    #args.log.info(f'processing in {args.etldir}')

    srcdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    srccon = dblib.DB(srcdb_dict, log=args.log, port = srcdb_dict.get('port',''))

    args.log.info('in run_sql')
    row_gen = srccon.fetchdict(sql)
    
    return logname,list(row_gen)

def process_results(args,results):
    for result in results:
        print(result)

def create_view_sql(args,table,present_ddl,vertica_order,rda1_dtype):
    '''


    '''

    new_rows=[]
    i=0
    for row in present_ddl.split('\n'):
        row=row.upper().strip()
        if not row: continue
        if 'CREATE OR REPLACE TABLE' in row: continue
        if row==');': continue
        row=row.strip(',')

        name,dtype=row.split(None,1)

        row = name
        if i<len(vertica_order):
            name=vertica_order[i]
            dtype=rda1_dtype[name]
            row=f'BWC_COL{i+1}::{dtype} AS {name}'

        i+=1
        new_rows.append(row)

    col_str='\n\t,'.join(new_rows)


    new_sql=f'''CREATE OR REPLACE VIEW {args.tgtdb.upper()}.{args.tgtschema.upper()}.{table} AS ( 
    SELECT
        {col_str}
    FROM    {args.presentdb.upper()}.{args.srcschema.upper()}.{table})
    '''

    return new_sql

def convert_ddl(ddl):
    '''
    create or replace TABLE ACCOUNT_TYPE (
        Z_META_TS_CDC TIMESTAMP_NTZ(9),
    )
    
    '''

    ddl_dict={} 
    i=0
    for row in ddl.split('\n'):
        row=row.upper().strip()
        if not row: continue
        if 'CREATE OR REPLACE TABLE' in row: continue
        if row==');': continue
        #if  row.startswith('BWC_'): continue
        name,dtype=row.split(None,1)
        dtype=dtype.strip(',')
        ddl_dict[name]=dtype

    return ddl_dict

def process_args():
    '''
    '''

    #etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"


    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--srcdir', required=True, help='tgt dir for data and logs')
    parser.add_argument( '--presentdb', required=True, help='tgt dir for data and logs') 
    parser.add_argument( '--datatypedb', required=True, help='tgt dir for data and logs') 
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument('--sql',required=False, help='default directory for all logs, data files')
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

        srcdb_dict=dbsetup.Envs[args.srcenv][args.srckey]
        srccon = dblib.DB(srcdb_dict, log=args.log, port = srcdb_dict.get('port',''))

        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
        tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = srcdb_dict.get('port',''))

        for table in tgtcon.get_tables(args.tgtschema,db=args.presentdb):
            #input(f'\n\nfix {args.tgtdb.upper()}.{args.tgtschema.upper()}.{table}')

            #table='SUMMARY_MONTHLY_AUTHORIZATION'
            try:
                col_order=srccon.get_cols(args.srcschema,table)
                dtype_ddl=tgtcon.get_ddl(args.srcschema,table,args.datatypedb)
            except:
                print(f' {table} info not avail, skipping')
                continue

            col_dtype=convert_ddl(dtype_ddl)
            present_ddl=tgtcon.get_ddl(args.srcschema,table,args.presentdb)
            sql=create_view_sql(args,table,present_ddl,col_order,col_dtype)
            tgtcon.exe(sql)
            args.log.info(f'finished {args.tgtdb.upper()}.{args.tgtschema.upper()}.{table}')

            src_sql=f' select * from {args.srcschema}.{table} order by 1,2,3 limit 3'
            rows=list(srccon.fetchdict(src_sql))
            vertica_csv=args.logdir/f'vertica_{table}.csv'
            print(f'writing to {vertica_csv}')
            inf.write_csv(vertica_csv,rows,delim=',')

            snow_csv=args.logdir/f'snow_{table}.csv'
            tgt_sql=f' select * from {args.tgtdb}.{args.tgtschema}.{table} order by 1,2,3 limit 3'
            rows=list(tgtcon.fetchdict(tgt_sql))
            inf.write_csv(snow_csv,rows,delim=',')

        #select get_ddl('TABLE','DBTEST.X10057301.ACCOUNT_TYPE');

        #result=srccon.get_cols_dtype(args.srcschema,db=args.srcdb)

                   
        # all_args=[(args,sql) for sql in sqls]
        # if args.parallel==1: 
        #     args.log.debug('Running in single threaded mode')
        #     results= [ main_parallel(allarg) for allarg in all_args ]
        # else:
        #     results=inf.run_parallel(main_parallel,args=all_args,parallel=args.parallel,log=args.log)

        # process_results(args,results)

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
python run_copy_tblddl2views.py  --srcdir /dev/vertica_etl/rpd1/dw_actuarial  --tgtdir /uat/snow_etl/uat_views/dw_actuarial --presentdb uat_present --datatypedb rda1_source --eldir c:/temp
python run_copy_tblddl2views.py  --srcdir /dev/vertica_etl/rpd1/ DW_EXTERNAL_RISK_CONTROL --tgtdir /uat/snow_etl/uat_views/ DW_EXTERNAL_RISK_CONTROL --presentdb uat_present --datatypedb rda1_source --eldir c:/temp




'''
