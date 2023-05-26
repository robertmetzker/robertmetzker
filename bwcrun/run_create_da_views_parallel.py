#std libs
from pickle import APPEND
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

    tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))

    args.log.info('in run_sql')
    row_gen = tgtcon.fetchdict(sql)
    
    return logname,list(row_gen)

def process_results(args,results):
    for result in results:
        print(result)

def process_args():
    '''
    '''

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument('--sql',required=False, help='default directory for all logs, data files')
    #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    #optional
    parser.add_argument('--eldir', required=False, default=eldir,help='default directory for all logs, data files')
    parser.add_argument('--parallel', required=False, default=15,help='number of parallel processes')
    #
    args= parser.parse_args()
    args.parallel=int(args.parallel)

    args.root=Path(__file__).resolve().parent.parent

    inf.build_args_paths(args)
    
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

def get_views(args):
    view_dir = args.root/f'bwcviews'/args.tgtenv/args.tgtdb/args.tgtschema
    sql_files=[]
    for afile in view_dir.iterdir():
        if afile.suffix != '.sql':
            continue
        sql=afile.read_text()
        sql=sql.replace('!ENV!',args.tgtenv.upper())
        sql_files.append(sql)
    return sql_files

def main():
    try:
        args=None
        args=process_args()
        views=get_views(args)

        all_args=[(args,sql) for sql in views]
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
dev
python run_create_da_views_parallel.py --tgtdir /uat/snow_etl/uat_views/BWCMISC --eldir c:/temp

Automic 
python run_create_da_views_parallel.py --tgtdir /uat/snow_etl/uat_views/BWCMISC
'''
