'''
   EXTRACT CSV data or DDL from a given schema based on command line arguments
   REQUIRED:  --srcdir >> env/conn/schema (e.g.  dev/vertica_me2/BWCODS )
   OPTIONAL:
        --raw           Just raw data dump by default: remove delimiter,term, and convert to ascii
        --rerun         Overwrite current data
        --ddl_only      Only extract DDLs, and changes rowcount for fetches to 1
        --full_schema   Extract all tables within the specified schema
        --skip         do no process tablea,tableb,tablec, etc       
        --etldir        default directory to place the files
        --delim         delimiter to use, e.g. ~ | ,
        --term          line terminator to use, defaults to \n  newline
        --parallel      num of extracts to do in parallel, default= 1 single threaded mode
        --logdir        default logging directory, $root/env/conn/schema/load_key/logs
        --load_key      load_key to use (defaults to current date as YYYY_MM_DD
        --inc_days      incremental number of days to use.  e.g. 14 would be 2 weeks based on key
        --sql           sample SQL to execute
        --table         specific table to extract when not doing full_schema, e.g. DATE_DIM
        --max_col       Maxmimum column width, default=64000
'''
# Standard Libraries shared/used by BWC
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
    libpath=str(prog_path.parent.parent.parent)
    sys.path.append(libpath)
    print('using path',libpath)

set_libpath()

import threading, queue
   
# Define Vertica column datatype mappings to Generic Mappings
def vertica2dict(args,col):
    '''
    https://github.com/vertica/vertica-python/blob/master/vertica_python/datatypes.py
    Column(name='HSPTL_ADMSN_CODE', type_code=9, display_size=1, internal_size=-1, precision=None, scale=None, null_ok=False)
    '''

    import vertica_python

    vtype=vertica_python.datatypes.VerticaType()
    num2type={}
    for v in dir(vtype): 
        if v.startswith('_'): continue
        num2type[str(getattr(vtype,v))]=v

    col_type=num2type[str(col.type_code)]

    if col_type in ('INT8','FLOAT8','NUMERIC'):
        new_type='NUMBER'
    elif col_type in ('CHAR', 'VARCHAR', 'BINARY',
                    'VARBINARY', 'UNKNOWN','LONGVARBINARY', 'LONGVARCHAR'):
        new_type='VARCHAR'
    elif 'DATE' in col_type:
        new_type='DATE'
    elif 'TIMESTAMP' in col_type:
        new_type='TIMESTAMP'
    elif 'TIME' in col_type:
        new_type='TIME'
    else:
        args.log.debug(f'unknown col, using varchar {col} {col_type}')
        new_type='VARCHAR' 

    new_col={}
    new_col['name']=col.name
    new_col['datatype']=new_type
    new_col['display_size']=col.display_size
    new_col['internal_size']=col.internal_size    
    new_col['scale']=col.scale
    new_col['precision']=col.precision
    new_col['null_ok']=col.null_ok
#    new_col['primary']=primary.get('primary_key','')   #Add to lookup primary query process
      
    return new_col


# Define Oracle column datatype mappings to Generic Mappings
def oracle2dict(args,col):
    '''
    #https://github.com/ibmdb/python-ibmdb/blob/master/IBM_DB/ibm_db/ibm_db_dbi.py
    #https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html
    name, type, display_size, internal_size, precision, scale, null_ok)
    ALPHA_DATE,<cx_Oracle.DbType DB_TYPE_DATE>,23,None,None,None,1
    '''
    #name, datatype, display_size, internal_size, precision, scale, null_ok= col

    new_col={}
    col_type=str(col[1])

    if 'DB_TYPE_CHAR' in col_type:
        new_type='VARCHAR' 
    elif 'DB_TYPE_VARCHAR' in col_type:
        new_type='VARCHAR' 
    elif 'DB_TYPE_NVARCHAR' in col_type:
        new_type='VARCHAR'  
    elif 'DB_TYPE_NUMBER' in col_type:
        new_type='NUMBER'
    elif 'DB_TYPE_DATE' in col_type:
        new_type='DATE'
    elif 'DB_TYPE_TIMESTAMP' in col_type:
        new_type='TIMESTAMP'
    elif 'DB_TYPE_BOOLEAN' in col_type:
        new_type='BOOLEAN'
    elif 'DB_TYPE_CLOB' in col_type:
        new_type='VARCHAR' 
    else:
        args.log.debug(f'unknown col, using varchar {col} {col_type}')
        new_type='VARCHAR' 

    new_col={}
    new_col['name']=col[0]
    new_col['datatype']=new_type
    new_col['display_size']=col[2]
    new_col['internal_size']=col[3]   
    new_col['scale']=col[4]
    new_col['precision']=col[5]
    new_col['null_ok']=col[6]
#    new_col['primary']=primary.get('primary_key','')   #Add to lookup primary query process

    return new_col


# Determine the DDL mapping to use (Oracle vs Vertica)
def get_ddl(args,table):
    '''
    '''
    args.log.debug('getting ddl')
    cols=[]

    for col in args.con.cursor.description:
        if args.tgtdb['type']=='vertica':
            col_dict=vertica2dict(args,col)
            cols.append(col_dict)
        elif args.tgtdb['type']=='oracle':
            col_dict=oracle2dict(args,col)
            cols.append(col_dict)
        else:
            raise ValueError(f"Unsupported Database {args.tgtdb['type']}")
        #cols.append(','.join(map(str,col)))
            
    return cols


def write_ddl_info(args,fname,table,row_gen):
    fields=[]
    for firstrow in row_gen:
        fields=list(sorted(firstrow.keys()))
        ddl_rows=get_ddl(args,table)
        ddl_file=args.tgtdir/'ddl'/f'{table}.csv'
        if ddl_file.exists() and not args.rerun:
            args.log.info(f'skipping DDL for {table} to:\n\t {ddl_file}')
            return True
        inf.write_csv(ddl_file,ddl_rows,log=args.log)
        args.log.debug(f'wrting DDL for {table} to: {ddl_file}')
        break
    if not fields:
        args.log.info(f'NO Data to extract for {table}')
        return False

    return True

# Output the CSV file based on Arguments.
# all arguments are passed, along with
# fname, table, row_gen (generated row of information)
# Changed call to inf.write_csv to include gz=FALSE (A85275)


# Begin to extract all information for a given schema/table based on
# Command line arguments.  
def dump_table(allargs):
    args=con=None
    src_count=0
    try:
        args,table_args=allargs
        prefix=args.prefix
        base_results={f'{prefix}full_fname':'',f'{prefix}fname':'',f'{prefix}status':'',f'{prefix}warning':'',f'{prefix}error':'',
        f'{prefix}rows':0, f'{prefix}src_count':0,f'{prefix}runtime':0,f'{prefix}rate':0,f'{prefix}start':'',f'{prefix}end':'',
        }
        

        table,table_dict=table_args
        args.log=inf.setup_log(args.logdir,app=f'child_{table}',prefix=(args.level+1)*'\t')
        fname=args.csvdir/f'{table}.csv.gz'
        base_results[f'{prefix}full_fname']=fname;base_results[f'{prefix}fname']=Path(fname).name

        args.log.info(f'\t----starting dump_table for: {args.schema}.{table}')

        if fname.exists() and not args.rerun: 
            args.log.info(f'SKIPPING: already finished {args.schema}.{table} found {fname}')
            base_results[f'{prefix}warning']='skipped'
            base_results[f'{prefix}status']='skipped'
            return base_results

        tgtdb = dbsetup.Envs[args.env][args.db]
        args.tgtdb=tgtdb
        con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
        args.con=con

        if args.sql:
            sql=args.sql
            args.log.debug(f'sql override found: {args.sql}')
        else:
            cols=con.get_cols(args.schema,table)
            if not cols: raise ValueError(f'No columns for {args.schema}.{table}')

            cols_str=','.join(cols)
            sql=f"select  {cols_str},\'{args.load_key}\' as BWC_DW_LOAD_KEY, \'{args.load_ts}\' as BWC_DW_EXTRACT_TS  from {args.schema}.{table}"
            #only do incremental if args.begin is defined
            if args.begin:
                args.log.debug(f'incremental defined: found: {args.begin}')
                audit=table_dict.get('audit')
                if audit:
                    args.log.debug(f'audit col found: {audit}')
                    sql=f"select {cols_str},\'{args.load_key}\' as BWC_DW_LOAD_KEY from {args.schema}.{table} where {audit} >  TO_DATE('{args.begin}', 'YYYY-MM-DD')"
                else:
                    args.log.debug(f'BULK LOAD, audit col NOT found: {audit}')

        count_sql= f''' with extract_query as ({sql}) select count(*) as COUNT from extract_query'''
        src_count=list(args.con.fetchdict(count_sql,size=1))[0]['COUNT']
        args.log.info(f'source table count ={count_sql}')
        base_results[f'{prefix}src_count']=src_count

        if not src_count:
             base_results[f'{prefix}warning']='empty table'
             if con: con.closedb();con=None
             return base_results

        row_gen= args.con.fetchdict(sql,size=1)
        gotddl=write_ddl_info(args,fname,table,row_gen)

        if gotddl:
            row_gen= args.con.fetchdict(sql)
            args.log.debug('writing table')
            write_dict= inf.write_csv(fname,row_gen,log=args.log)
            #add values write_csv doesn't know about
            write_dict[f'{prefix}src_count']=src_count 
            #write_dict['load_key']=args.load_key
            if con: con.closedb();con=None
            args.log.info(f'\t----Child done {args.schema}.{table} write_dict')
            return write_dict
        else:
            args.log.error(f'\t----Child done {args.schema}.{table}')
            raise AssertionError(f'NO DDL for table {args.schema}.{table}')
            return base_results

    except:
        errmsg=inf.geterr()
        base_results[f'{prefix}error']=errmsg
        base_results[f'{prefix}status']='error'
        if args: args.log.error(errmsg)
        else: print(errmsg)
        if con: con.closedb();con=None
        args.log.info(f'\t----Child done {args.schema}.{table}')

    return base_results

# Parse the command line arguments
def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    codedir=str(Path(f"{os.environ['USERPROFILE']}")/'bwcroot/bwcenv')

#    envs=extract_info.Envs.keys()
#    dbs=extract_info.Envs[list(envs)[0]].keys()
 
    try:
        parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --srcdir /dev/cam/base",add_help=True)
        #required
        parser.add_argument( '--srcdir', required=True,help='/env/database/schema/[table]')
        #boolean
        parser.add_argument( '--raw', default=False,action='store_true',help='Just raw data dump by default: remove delimiter,term, and convert to ascii')
        parser.add_argument( '--rerun', default=False,action='store_true',help='Over write current data')
        parser.add_argument( '--ddl_only',default=False,action='store_true',help='Only extract DDLs, and changes rowcount for fetches to 1')
        parser.add_argument( '--full_schema',default=False,action='store_true',help='Extract all tables within the specified schema')
        #optional
        parser.add_argument( '--skip', default='',help='comma list of tables to skip')
        parser.add_argument( '--level', default=1,help='where in call chain this is for logging')
        parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
        parser.add_argument( '--delim', default='\t',help='delim to use, e.g. ~ | ')
        parser.add_argument( '--term', default='\n',help='line terminator to use, defaults to \n  newline')
        parser.add_argument( '--parallel', default=5,type=int,help='num of extracts to do in parallel, default= 1 single threaded mode ')
        parser.add_argument( '--logdir', default='',help='default logging directory, $root/env/conn/schema/load_key/logs')
        parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
        parser.add_argument( '--inc_days', default=0,type=int,help='incremental number of days to use.  e.g. 14 would be 2 weeks based on key')
        parser.add_argument( '--sql',help='sample SQL to execute')
        parser.add_argument( '--table',help='specific table to extract when not doing full_schema, e.g. DATE_DIM')
        parser.add_argument( '--max_col',default=64000,type=int,help='Maxmimum column width, default=64000')
        args = parser.parse_args()
    except:
        print(inf.geterr())
        print(f'Example: python  {sys.argv[0]} --srcdir /dev/vertica_me2/bwcods --ddl_only --full_schema ')
 #       print(f'envs:{envs} Databases:{dbs}')
        return None

    #setup schema,db,env
    args.schemadir=Path(args.eldir)/args.srcdir.strip('/').strip('\\')
    args.srcdir=Path(args.srcdir)
    args.schema=args.srcdir;args.db=args.schema.parent;args.env=args.db.parent
    args.env=args.env.name.lower(); args.db=args.db.name.lower();args.schema=args.schema.name.lower()
    args.prefix=''

    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key=ymd
    args.load_ts=ymd_hms

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.tgt_data=args.schemadir/'data'
    args.tgt_log=args.schemadir/'logs'
    args.logdir=args.tgt_log/args.load_key
    args.tgtdir=args.tgt_data/args.load_key
    args.ddldir=args.tgtdir/'ddl'
    args.csvdir=args.tgtdir/'extracts'
    args.loaddir=args.tgtdir/'loads'
    args.ddldir=args.tgtdir/'ddl'


 # Directories will be created if they do not already exist.
    args.ddldir.mkdir(parents=True, exist_ok=True)
    args.csvdir.mkdir(parents=True, exist_ok=True)
    args.logdir.mkdir(parents=True, exist_ok=True)
    args.loaddir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app='parent',prefix=args.level*'\t')
    args.log.info(f'{sys.argv[0]} launched with arguments: {sys.argv[1:]}')
    args.log.debug(f'Saving files to:{args.ddldir} {args.csvdir} {args.logdir}')

    args.begin=''
    if args.inc_days:
        timedelta=datetime.timedelta(days=args.inc_days)
        begin_date=now-timedelta
        args.begin=begin_date.strftime('%Y-%m-%d')
    args.log.debug('Running on:'+os.environ['COMPUTERNAME'])
    args.log.debug(f'import using {localdir}')
    args.log.debug(f'args global settings:{args}')
    args.log.info(f'Incremental mode:{args.begin}')

    return args

def get_tables(args):
    tgtdb = dbsetup.Envs[args.env][args.db]
    con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    tables=con.get_tables(args.schema)
    return tables


# Process and check to see if  full_schema has been chosen.  
# If not, then parse out the tables as a list, using comma as a separator
# Determine the Parallelism to use when running.  Default = 1

def process_reports(args):
    '''
    {'end': '20210628-15:49:08', 'error': '', 'fname': 'account_type.csv.gz', 'full_fname': 'I:\\....account_type.csv.gz',
     'parent_end': '20210628-15:49:18', 'parent_runtime': '14.16', 'parent_start': '20210628-15:49:04', 
     'rate': '3', 'rows': '2', 'runtime': '0.56', 'src_count': '1', 'start': '20210628-15:49:08', 'warning': ''
     }
    '''
    prefix=args.prefix
    alltables={}
    for afile in sorted(args.logdir.iterdir()):
        if not afile.name.startswith('report_'): continue
        rows=list(inf.read_csv(afile))
        for row in rows:
            args.log.debug(row)
            fname=row[f'{prefix}fname']
            status=row[f'{prefix}status']
            warning=error=0
            if row[f'{prefix}error']: error=1
            if row[f'{prefix}warning']: warning=1

            #
            if fname not in alltables: 
                row[f'{prefix}error_runs']=error
                row[f'{prefix}warning_runs']=warning
                row[f'{prefix}attempt']=1
                alltables[fname]=row

            if status == 'skipped' or  status == '':
                alltables[fname][f'{prefix}attempt']+=1
                alltables[fname][f'{prefix}warning_runs']+=warning
                alltables[fname][f'{prefix}error_runs']+=error
            else:
                #reset, because table was written out again
                row[f'{prefix}error_runs']=alltables[fname][f'{prefix}error_runs']+error
                row[f'{prefix}warning_runs']=alltables[fname][f'{prefix}warning_runs']+warning
                row[f'{prefix}attempt']=alltables[fname][f'{prefix}attempt']+1
                alltables[fname]=row

    row_dicts=[v for k,v in sorted(alltables.items())]

    report=f'{args.logdir}/final_report_{Path(sys.argv[0]).name}.csv'
    inf.write_csv(report,row_dicts,log=args.log,delim=',')


def main():
    try:
        begin=datetime.datetime.now()
        args=process_args()
        if args==None: return 1

        skip_tables={}
        if args.skip:
            skip_tables={atable.upper() for atable in args.skip.split(',')}

        if args.full_schema:
            tables = { table:{} for table in get_tables(args) if table.upper() not in skip_tables}
        elif not args.table:
            tables=extract_info.Envs[args.env][args.db][args.schema]
        else:
            tables={ table:{} for table in args.table.split(',') }

        final_tables={}
        for table,table_dict in tables.items():
            if table.upper() in skip_tables: 
                args.log.info('skipping:'+table)
                continue
            final_tables[table]=table_dict
            

        if len(final_tables)==0:
            args.log.info('no tables to process;exiting')
            sys.exit(1)

        if len(final_tables)<args.parallel: 
            args.log.debug(f'dropping parallel to {len(final_tables)}')
            args.parallel=len(final_tables)
        args.log.info(f'=== Processing {len(final_tables)} tables using parallel={args.parallel}')
        args.log.debug(f'{final_tables}')

        all_args=[(args,table_args) for table_args in  final_tables.items()]
        if args.parallel==1: 
            args.log.debug('Running in single threaded mode')
            results= [ dump_table(allarg) for allarg in all_args ]
        else:
            results=inf.run_parallel(dump_table,args=all_args,parallel=args.parallel,log=args.log)

        process_results(args,begin,results)

    except:
        args.log.error(inf.geterr())
        sys.exit(1)
if __name__=='__main__':
    main()