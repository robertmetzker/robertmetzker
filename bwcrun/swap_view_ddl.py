#!/usr/bin/env python
#std libs
import sys, argparse, os, datetime, random, re
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

from bwcenv.bwclib import dblib, inf
from bwcenv.bwclib.inf import write_csv
from bwcsetup import dbsetup


def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    etldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"

    parser = argparse.ArgumentParser(description='command line args', epilog="Example:python extract.py --srcdir /dev/cam/base", add_help = True)
    #required
    parser.add_argument( '--srcdir', required = True, help = 'Source view /env/key/[database]/[schema]')
    parser.add_argument( '--tgtdir', required = True, help = 'Target view /env/key/[database]/[schema]')

    #boolean
    # parser.add_argument( '--limit_rows',default=False,action='store_true',help='Set row limit, default=10')

    #optional
    parser.add_argument( '--fr', default = '', help = 'source schema to move FROM, e.g. PCMP')
    parser.add_argument( '--to', default = '', help = 'source schema to move TO, e.g.   PCMP8')
    
    parser.add_argument( '--eldir', default = etldir, help = 'default directory to dump the files')
    parser.add_argument( '--output', default ='', help = 'default output directory to dump the files')
    parser.add_argument( '--load_key', default = '', help = 'load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--logdir', default = '', help = 'default logging directory, $root/env/conn/schema/load_key/logs')
    
    args = parser.parse_args()
 
    args.etldir = Path( args.output ) if args.output else Path( etldir )

    #-- set the load key if not specified
    now = datetime.datetime.now()
    args.now = now
    ymd = now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms = now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');

    args.srcdir = args.srcdir.strip('/')
    if args.srcdir.count("/") == 3:
        args.srcenv, args.srckey, args.src_db, args.src_schema  = args.srcdir.split('/')
    elif args.srcdir.count('/') == 2:
        args.srcenv, args.srckey, args.src_db = args.srcdir.split('/')
        args.src_schema = None
    else:
        args.src_db, args.src_schema = None, None
        args.srcenv, args.srckey = args.srcdir.split('/', 1 )

    if args.tgtdir:
        args.tgtdir = args.tgtdir.strip('/')
        if args.tgtdir.count("/") == 3:
            args.tgtenv, args.tgtkey, args.tgt_db, args.tgt_schema  = args.tgtdir.split('/')
        elif args.tgtdir.count('/') == 2:
            args.tgtenv, args.tgtkey, args.tgt_db = args.tgtdir.split('/')
            args.tgt_schema = None
        else:
            args.tgt_db, args.tgt_schema = None, None
            args.tgtenv, args.tgtkey = args.tgtdir.split('/', 1 )

    #alternative way to determine load file location
    # inf.build_args_paths(args, use_load_key = False, find_src_load_key = False)
    args.srcdir = Path( 'c:/TEMP')
    args.srcdata = args.srcdir/'data'  
    args.srclog = args.srcdir/'logs'

    args.prefix=''

    args.root=Path(__file__).resolve().parent.parent

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir = args.srclog

 # Directories will be created if they do not already exist.
    args.level = 1
    args.log = inf.setup_log(args.logdir, app='parent', prefix = args.level*'\t')
    args.log.debug(f'INFO: args global settings:{args}')

    return args


def get_db_views( args, srccon ):
    schemas_w_views = []
    view_info = []
    schema_src_sql = '''with src as ( select * from information_schema.views where table_schema not in ('INFORMATION_SCHEMA'))
select distinct table_schema from src'''
    results = srccon.fetchdict( schema_src_sql )

    for schema in results:
        schemas_w_views.append( schema['TABLE_SCHEMA'] )

    for schema in schemas_w_views:
        srccon.fetchone(f"USE schema {schema}") #If you want to use different DataBase for example DWH_DEV
        row_gen = srccon.fetchdict( 'SHOW VIEWS')
        view_info = view_info + [ row for row in row_gen ]
        print( view_info )
        
    return view_info


def replace( old, new, str, caseinsensitive = True ):
    if caseinsensitive:
        return re.sub( old, new, str, flags = re.IGNORECASE)
    else:
        return str.replace( old, new)


def main(): 
    '''
    Get a file containing the output of a schema
    '''
    args=process_args()
    src_views, view_sql = [], []

    print(dbsetup.Envs[args.srcenv])
    # args.srcenv, args.srckey, args.srcdb, args.srcschema=args.srcdir.split('/')
    srcdb = dbsetup.Envs[args.srcenv][args.srckey]
    srccon = dblib.DB(srcdb, log = args.log, port = srcdb.get('port',''))

    print( f'Launched with:', str(sys.argv) )    
    srccon.fetchone("USE warehouse WH_BI") #If you want to use different WareHouse for example SNOWFLAKE_XS_BI

    if args.src_db:
        srccon.fetchone(f"USE database {args.src_db}") #If you want to use different DataBase for example DWH_DEV

    if args.src_schema:
        srccon.fetchone(f"USE schema {args.src_schema}") #If you want to use different DataBase for example DWH_DEV
        row_gen = srccon.fetchdict( 'SHOW VIEWS')
    else:
        # create a process to find all schemas that contain views, then build the views from those
        row_gen = get_db_views( args, srccon )


    # {'created_on': datetime.datetime(2022, 10, 14, 11, 24, 38, 618000, tzinfo=<DstTzInfo 'America/New_York' EDT-1 day, 20:00:00 DST>), 
    # 'name': 'ABSENCE_TYPE_CD', 'reserved': '', 'database_name': 'DEV_VIEWS', 'schema_name': 'PCMP', 'owner': 'DEV_BATCH', 'comment': '',
    # 'text': 'CREATE  OR REPLACE VIEW  DEV_VIEWS.PCMP.ABSENCE_TYPE_CD AS (\n\n                select \n                ABSNC_TYP_CD, ABSNC_TYP_NM, AUDT_USER_CREA_DTM, AUDT_USER_CREA_ID, AUDT_USER_UPDT_DTM, AUDT_USER_UPDT_ID, BWC_DW_EXTRACT_TS, BWC_DW_LOAD_KEY, EFF_DTM, END_DTM, BWC_ID\n                from  DEV_PRESENT.PCMP.ABSENCE_TYPE_CD\n        )', 
    # 'is_secure': 'false', 'is_materialized': 'false'}
    desired_fields = ['database_name', 'schema_name', 'name', 'text', 'comment']
    for row in row_gen:
        # Skip the view if it is a BWC version by first regexp removing all numbers at the end and then looking for _BWC:
        testname = re.sub( r'\d+$','', row['name'] )
        if not testname.endswith('_BWC'): 
            row_dict = {}
            sql_only = {}
            for field in desired_fields:
                if field == 'text' and args.fr and args.to:
                    temp = row[field]
                    fr = ' '+ args.fr +'.'
                    to = ' '+ args.to +'.'
                    if args.tgt_db:  
                        # temp = temp.replace( row_dict['database_name'] +'.', args.tgt_db +'.' )
                        temp = replace( row_dict['database_name'] +'.', args.tgt_db +'.', temp )
                    if args.tgt_schema:  
                        # temp = temp.replace( row_dict['schema_name'] +'.', args.tgt_schema +'.' )                                    
                        temp = replace( row_dict['schema_name'] +'.', args.tgt_schema +'.', temp )                                    
                    # temp = temp.replace( fr, to ) 
                    temp = replace( fr, to, temp ) 
                    row_dict[field] = temp  
                else:
                    row_dict[field] = row[field]
            src_views.append( row_dict )
            
            sql_only['--VIEW_SQL'] = row_dict['text'] +';' 
            view_sql.append( sql_only )

    print( src_views )

    print( f'===> Comepleted execution of {" ".join( sys.argv )}' )
    print( f'Launched with:', str(sys.argv) )    

    # inf.write_csv( fname="c:/temp/views.sql", rows=src_views )
    fname = f"C:/TEMP/{args.tgt_db}_{'ALL' if not args.tgt_schema else args.tgt_schema}_from_{args.src_db}_{'ALL' if not args.src_schema else args.src_schema}_views.sql"
    inf.write_csv( fname=fname, rows = view_sql  )

    print( f'==> WROTE views to: {fname}')
    
if __name__ == '__main__':
    print("process Begin")
    main()
    print("Process End")


# python swap_view_ddl.py --srcdir /prd/snow_etl/PRD_VIEWS/PCMP  --tgtdir /dev/snow_etl/DEV_VIEWS/PCMP --fr PRD_PRESENT --to DEV_PRESENT
# python swap_view_ddl.py --srcdir /prd/snow_etl/PRD_VIEWS/PCMP  --tgtdir /uat/snow_etl/UAT_VIEWS/PCMP --fr PRD_PRESENT --to UAT_PRESENT
# python swap_view_ddl.py --srcdir /prd/snow_etl/PRD_VIEWS/PCMP8  --tgtdir /uat/snow_etl/UAT_VIEWS/PCMP8 --fr PRD_PRESENT --to UAT_PRESENT
# python swap_view_ddl.py --srcdir /uat/snow_etl/UAT_VIEWS/DW_REPORT  --tgtdir /prd/snow_etl/PRD_VIEWS/DW_REPORT --fr RDA1_SOURCE --to RPD1
# python swap_view_ddl.py --srcdir /dev/snow_etl/DEV_VIEWS  --tgtdir /uat/snow_etl/UAT_VIEWS --fr DEV_PRESENT --to UAT_PRESENT
