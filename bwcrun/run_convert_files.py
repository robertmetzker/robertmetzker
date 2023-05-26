import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime,shutil
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

from bwcenv.bwclib import inf

def find_files_for_load(adir):
    print(pdir)


def archive_src_files(args, archdir, found_files):
    args.log.info(f'Found {len(found_files)} in {found_files[0].parent} archiving to {archdir}')
    archdir.mkdir(parents = True, exist_ok = True)
    for afile in found_files:
        arch_file = archdir/afile.name
        afile.replace(arch_file)


def process_args():

    #eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    #codedir=str(Path(f"{os.environ['USERPROFILE']}")/'bwcroot/bwcenv')

    parser = argparse.ArgumentParser(
        description='command line args', epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00", add_help=True)
    # required
    parser.add_argument( '--srcdir', required=True, help='src dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    #boolean
    parser.add_argument( '--archive', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    # optional
    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')

    args = parser.parse_args() 
    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srcdata, etc.
    #use_load_key=False, find_src_load_key=False 
    #inf.build_args_paths(args)
    args.srcdir=Path(args.srcdir)
    args.tgtdir=Path(args.tgtdir)
    print(args.tgtdir)
    args.logdir=Path(args.eldir)/"EXTRACTS"/"KRONOS"
    args.log=inf.setup_log(args.logdir,app='parent')

    return args


def main():
    args = process_args()
    # Find Files
    found_files = []
    args.log.info(f'Running rglob in {args.srcdir}')
    xls_files=list(args.srcdir.rglob("*.xls"))
    args.log.info(f'Found {len(xls_files)} files')
    for xls in xls_files:
        found_files.append(xls)
        csv_file = xls.with_suffix('.csv')
        tgt_file = args.tgtdir/(csv_file.name)
        args.log.info(f'converting {xls} to {tgt_file}')
        inf.write_csv(tgt_file, inf.fetchdict_file( xls ), sortit=False )


    if args.archive:
        archdir = args.srcdir/'archive'
        archive_src_files( args, archdir, found_files )


    args.log.info('-==# END LOG #==-')


if __name__ == '__main__':
    main()


'''
Production:
    python "E:/Release_UAT/bwcroot/bwcenv/bwcrun"/run_convert_files.py --srcdir "\\testmswg9\testgroups\Cognos\Data_Lake\Kronos\KRONOS_TMKP" --tgtdir "\\testmswg9\testgroups\Cognos\Enterprise\Test\File Transfer\CSV File To Many\" --archive
    python.exe run_convert_files.py --srcdir I:/Data_Lake/Kronos/KRONOS_TMKP --tgtdir I:/Enterprise/Production/File Transfer/CSV File To Many --archive
Test:
    python.exe run_convert_files.py --srcdir I:/Data_Lake/Kronos/KRONOS_TMKP --tgtdir I:/Data_Lake/Kronos/KRONOS_TMKP --archive  
'''
