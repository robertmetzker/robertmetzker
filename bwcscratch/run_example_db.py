#std libs
import sys, argparse, os, datetime, random
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
    prog_path = Path(os.path.abspath(__file__))
    root = prog_path.parent.parent.parent
    pyversion = f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup


def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='command line args', epilog="Example:python extract.py --srcdir /dev/cam/base", add_help = True)
    #required
    parser.add_argument( '--srcdir', required = True, help = '/env/database/schema/[table]')
    parser.add_argument( '--sql', required = False, help = 'sql statement to execute')

    #boolean
    # parser.add_argument( '--limit_rows',default=False,action='store_true',help='Set row limit, default=10')

    #optional
    parser.add_argument( '--eldir', default = eldir, help = 'default directory to dump the files')
    parser.add_argument( '--level', default = 1, help = 'where in call chain this is for logging')
    parser.add_argument( '--load_key', default = '', help = 'load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--logdir', default = '', help = 'default logging directory, $root/env/conn/schema/load_key/logs')
    parser.add_argument( '--table', help = 'specific table to extract when not doing full_schema, e.g. DATE_DIM')

    
    args = parser.parse_args()


    #-- set the load key if not specified
    now = datetime.datetime.now()
    args.now = now
    ymd = now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms = now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');

    #alternative way to determine load file location
    inf.build_args_paths(args, use_load_key = False, find_src_load_key = False)
    args.prefix=''

    args.root=Path(__file__).resolve().parent.parent

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir=args.srclog

 # Directories will be created if they do not already exist.
    args.log = inf.setup_log(args.logdir, app='parent', prefix=args.level*'\t')
    args.log.debug(f'INFO: args global settings:{args}')

    return args


def main():

    args=process_args()

    print(dbsetup.Envs[args.srcenv])
                # args.srcenv, args.srckey, args.srcdb, args.srcschema=args.srcdir.split('/')
    srcdb = dbsetup.Envs[args.srcenv][args.srckey]
    srccon = dblib.DB(srcdb, log=args.log, port = srcdb.get('port',''))

    #row_gen = srccon.fetchdict(sql)
    row_gen = srccon.fetchdict(args.sql)
    for row in row_gen:
        print('***', row)
        break

    for row in srccon.get_tables('dbo'):
        print(row)
        break

    for row in srccon.get_objects('dbo'):
        print('row')
        break
    
    adict=srccon.get_cols('dbo')
    print(adict)
        #break

    alist=srccon.get_cols('dbo','yes_or_no')
    print(alist)

    alist=srccon.get_cols_dtype('dbo','yes_or_no')
    print(alist)

    #{'ANSWER': 'nvarchar', 'KEY': 'bit'}
    for col in srccon.cursor.description:
        print(col, '**')

    args.log.info(f'done')



if __name__=='__main__':
    main()

'''
Example execution:
    python run_example_db.py --srcdir /dev/oracle_etl/DB/Schema --sql "select *  from pcmp.common_change_tracking where rownum <=2"
    python run_example_db.py --srcdir /dev/sql_etl_dep/DEP/dbo --sql "select *  from dep.dbo.UserPermission "
    python run_example_db.py --srcdir /prd/oracle_etl/pub1/pcmp
'''    
