#std libs
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime,shutil
from pathlib import Path
from winreg import LoadKey

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

def get_data_dirs(args):
    r'''
    afile=C:\Temp\a75551\EL\uat\snow_etl\RUB1\DW_REPORT\data\2022_09_25PM
    schema_dir=C:\Temp\a75551\EL\uat\snow_etl\RUB1\DW_REPORT
    data_dir=C:\Temp\a75551\EL\uat\snow_etl\RUB1\DW_REPORT\data
    '''
    datadirs={}
    print('args envdir rglob',args.envdir.rglob)
    for afile in args.envdir.rglob('*.gz'):
        schema_dir=afile.parent.parent.parent
        data_dir=afile.parent.parent
        loadkey_dir=afile.parent

        if schema_dir not in datadirs:
            datadirs[schema_dir]=set()

        datadirs[schema_dir].add(loadkey_dir)
    
    all_deletes=[]

    for schema_dir,loadkey_set in datadirs.items():
        sorted_loadkey=list(sorted(loadkey_set))
        todel=[schema_dir/loadkey for loadkey in sorted_loadkey[:-3]]
        all_deletes.extend(todel)

    return all_deletes    

def process_args():
    '''

    '''
    #etldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    #eldir=f"E:/EXTRACTS/svc_automic/EL"


    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
    #required
    parser.add_argument( '--env', required=True,help='data source /env/mf/cam_layouts')

    #optional
    parser.add_argument( '--eldir', default=eldir,help='default directory for logging, data files, etc')
    parser.add_argument( '--dev', default=False, action='store_true', help='Changes ETL Dir from C to E if true')

    args = parser.parse_args()

    if args.dev: eldir=f"C:/temp/{os.environ['USERNAME'].replace('_x', '')}/EL/"
    args.eldir = Path( eldir )

    args.logdir=Path(args.eldir)/'logs'
    args.envdir=Path(args.eldir)/args.env

    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'processing in:{args.eldir}')
    #args.log.info(f'the environment:{inf.getenv()}')

    return args

def main():
    r'''
    C:\EXTRACTS\svc_automic\EL\uat\db2pt\dbpt\dbmoit00\data
    '''
    try:
        args=None
        args=process_args()
        todel=get_data_dirs(args)
        for adir in todel:
            if 'data' not in str(adir):
                args.log.info(f'skipping1 {adir}');continue
            if not adir.name.startswith('2') and not adir.parent.name.startswith('2'):
                args.log.info(f'skipping2 {adir}');continue
            args.log.info(f'deleting {adir}')
            shutil.rmtree(adir)
        args.log.info(f'done')
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
    python run_cleanup_files.py --env uat --dev
    python run_cleanup_files.py --env prd
    python run_cleanup_files.py --env uat
    python run_cleanup_files.py --env dev
'''