#std libs
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
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

from bwcenv.bwclib import inf
#from bwcsetup import dbsetup

def process_args():
    '''

    '''
    #etldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

 
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
    #required
    parser.add_argument( '--test', required=True,help='data source /env/mf/cam_layouts')

    #optional
    parser.add_argument( '--eldir', default=eldir,help='default directory for logging, data files, etc')
    args = parser.parse_args()

    args.logdir=Path(args.eldir)/'logs'

    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'processing in:{args.eldir}')
    #args.log.info(f'the environment:{inf.getenv()}')

    return args



def main():

    try:
        args=None
        args=process_args()
        print('got',args)
        args.log.debug('debug message')
        args.log.info('info message')
        args.log.warning('warn message')
        args.log.error('error message')
        args.log.critical('critical message')

        1/0
    
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
Notes

Note what happens on i drive with different argument w\args.schemadir/'logs'
    - python run_example2.py --srcdir /dev/vertica_dev1/pcmp
    - python run_example2.py --srcdir /dev/vertica_dev1/pcmp
    python run_example.py --test hello --eldir c:/temp

'''