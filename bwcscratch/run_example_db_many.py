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
    args.log.info(f'processing in {args.etldir}')

    tgtdb = dbsetup.Envs[args.env][args.db]
    con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))

    args.log.info('in run_sql')
    row_gen = con.fetchdict(sql)
    


    for idx,row in enumerate(row_gen):
        if idx>2: break
        print(row)
    return logname


def process_args():
    '''
    '''

    #etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"


    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument('--sql',required=False, help='default directory for all logs, data files')
    #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    #optional
    parser.add_argument('--eldir', required=False, default=eldir,help='default directory for all logs, data files')
    #
    args= parser.parse_args()

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

        print(dbsetup.Envs[args.tgtenv])

        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
        tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))
        sqls=[
            f'select * from {args.tgtdb}.{args.tgtschema}.ACCOUNT_STATUS_TYPE limit 10',
            f'select * from {args.tgtdb}.{args.tgtschema}.AGREEMENT limit 10',
            f'select * from {args.tgtdb}.{args.tgtschema}.PARTICIPATION limit 10',
            f'select * from {args.tgtdb}.{args.tgtschema}.TASK_HISTORY limit 10',
        ]
        all_args=[]

        for sql in sqls:
            table_args=[args,sql]
            row_gen = tgtcon.fetchdict(sql)
            for row in row_gen: 
                print( row )
            
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
python run_example_db_many.py --tgtdir /uat/snow_etl/uat_views/BWCMISC --eldir c:/temp
python run_example_db_many.py --tgtdir /uat/snow_etl/uat_views/pcmp --eldir c:/temp
'''
