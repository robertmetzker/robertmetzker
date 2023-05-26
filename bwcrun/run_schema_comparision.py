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
    libpath=str(prog_path.parent.parent.parent)
    sys.path.append(libpath)
    print('using path',libpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

def process_args():
    '''
    '''

    eldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"

    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument('--srcdir',required=True,help='hi again')
    parser.add_argument('--tgtdir',required=True,help='hi again')
    #boolean

    #optional
    parser.add_argument('--eldir', default=eldir,help='default directory for all logs, data files')
    #
    args= parser.parse_args()

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srcdata
    #use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args) 

    args.env,args.db,args.schema=args.srcdir.split('/')[1:]

    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'processing in {args.etldir}')

    return args

def run_sql(all_args):
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


def main():

    args=process_args()
    if args==None: return 1


    print(dbsetup.Envs[args.env])

    sqls=[
        f'select * from {args.schema}.ACCOUNT_TYPE',
        f'select * from {args.schema}.AGREEMENT',
        f'select * from {args.schema}.PARTICIPATION',
        f'select * from {args.schema}.TASK_HISTORY',     
    ]


    tgtdb = dbsetup.Envs[args.env][args.db]
    con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))


    for sql in sqls:
        
        row_gen = con.fetchdict(sql)
        for row in row_gen: break
        print(con.cursor.description)
        break


if __name__=='__main__':
    main()
