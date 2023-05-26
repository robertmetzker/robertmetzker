'''
   EXTRACT CSV data or DDL from a given schema based on command line arguments
   REQUIRED:  --srcdir >> env/conn/schema (e.g.  dev/vertica_me2/BWCODS )
   OPTIONAL:
        --raw           Just raw data dump by default: remove delimiter,term, and convert to ascii
        --rerun         Overwrite current data
        --ddl_only      Only extract DDLs, and changes rowcount for fetches to 1
        --full_schema   Extract all tables within the specified schema
        --skip          do not process tablea,tableb,tablec, etc       
        --etldir        default directory to place the files
        --delim         delimiter to use, e.g. ~ | ,
        --term          line terminator to use, defaults to \n  newline
        --parallel      num of extracts to do in parallel, default= 1 single threaded mode
        --logdir        default logging directory, $root/env/conn/schema/load_key/logs
        --load_key      load_key to use (defaults to current date as YYYY_MM_DD
        --inc_days      incremental number of days to use.  e.g. 14 would be 2 weeks based on key
        --table         specific table to extract when not doing full_schema, e.g. DATE_DIM
        --max_col       Maxmimum column width, default=64000
        --varchar_id    Set columns ending in "_ID" to varchar datatype
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
    root=prog_path.parent.parent.parent
    pyversion=f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

import threading, queue
   
# Define Vertica column datatype mappings to Generic Mappings
def snow2dict(args,col):
    '''
    https://docs.snowflake.com/en/user-guide/python-connector-api.html#description
    https://docs.snowflake.com/en/user-guide/python-connector-api.html#type-codes

    '''
    new_col={}
    col_type=col.type_code
    if col_type == 0:
        new_type='NUMBER'#FIXED
    elif col_type ==1:
        new_type='NUMBER'#REAL
    elif col_type ==2:
        new_type='VARCHAR'#TEXT
    elif col_type ==3:
        new_type='DATE'
    elif col_type ==4:
        new_type='TIMESTAMP'
    elif col_type ==5:
        new_type='VARCHAR'#VARIANT
    elif col_type ==6:
        new_type='TIMESTAMP'#TIMESTAMP_LTZ
    elif col_type ==7:
        new_type='TIMESTAMP'#TIMESTAMP_TZ
    elif col_type ==8:
        new_type='TIMESTAMP'#TIMESTAMP_NTZ
    elif col_type ==9:
        new_type='VARCHAR'#OBJECT
    elif col_type ==10:
        new_type='VARCHAR'#ARRAY
    elif col_type ==11:
        new_type='VARCHAR'#BINARY
    elif col_type ==12:
        new_type='TIME'
    elif col_type ==13:
        new_type='BOOLEAN'#BOOLEAN
    else:
        args.log.debug(f'unknown col, using varchar {col} {col_type}')
        new_type='VARCHAR'

    print(new_type)
    new_col={}
    new_col['name']=col.name
    new_col['datatype']=new_type
    new_col['display_size']=col.display_size
    new_col['internal_size']=col.internal_size    
    new_col['scale']=col.scale
    new_col['precision']=col.precision
    new_col['null_ok']=col.is_nullable
#    new_col['primary']=primary.get('primary_key','')   #Add to lookup primary query process
      
    return new_col
    




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

    ##added code to optionally force "_ID" columns to be VARCHAR due to tableau join issues
    if col_type in ('INT8','FLOAT8','NUMERIC') and col.name.endswith('_ID') and args.varchar_id:
        new_type='VARCHAR'
    elif col_type in ('INT8','FLOAT8','NUMERIC'):
        new_type='NUMBER'
    elif col_type in ('CHAR', 'VARCHAR', 'BINARY','VARBINARY', 'UNKNOWN','LONGVARBINARY', 'LONGVARCHAR'):
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
    elif 'DB_TYPE_BINARY_FLOAT' in col_type:
        new_type='NUMBER'
    elif 'DB_TYPE_BINARY_INTEGER' in col_type:
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
    new_col['precision']=col[4]
    new_col['scale']=col[5]
    new_col['null_ok']=col[6]
#    new_col['primary']=primary.get('primary_key','')   #Add to lookup primary query process

    return new_col

def db22dict(args,col):
    '''
    #https://github.com/ibmdb/python-ibmdb/blob/master/IBM_DB/ibm_db/ibm_db_dbi.py
    #https://cx-oracle.readthedocs.io/en/latest/api_manual/module.html
    name, type, display_size, internal_size, precision, scale, null_ok)
    [['BSRA_CRT_DTTM', DBAPITypeObject
    ({'TIMESTAMP'}), 26, 26, 26, 6, False], ['CRT_USER_CODE', DBAPITypeObject
    ({'CHAR VARYING', 'CHARACTER', 'CHARACTER VARYING', 'STRING', 'CHAR', 'VARCHAR'}), 8, 8, 8, 0, False], ['DCTVT_DTTM', DBAPITypeObject
    ({'TIMESTAMP'}), 26, 26, 26, 6, False], ['FK_CTPY_CRT_DTTM', DBAPITypeObject
    ({'SMALLINT', 'INTEGER', 'INT'}), 6, 6, 5, 0, False], ['FK_PYRT_CODE', DBAPITypeObject
    ({'DEC', 'DECIMAL', 'NUMERIC', 'NUM'}), 7, 7, 5, 4, False], ['MDCL_RATE', DBAPITypeObject
        '''
    #name, datatype, display_size, internal_size, precision, scale, null_ok= col

    new_col={}
    col_type=str(col[1])

    if 'CHARACTER' in col_type:
        new_type='VARCHAR'
    else:
        args.log.debug(f'unknown col, using varchar {col} {col_type}')
        new_type='VARCHAR' 

    new_col={}
    new_col['name']=col[0]
    new_col['datatype']=new_type
    new_col['display_size']=col[2]
    new_col['internal_size']=col[3]   
    new_col['precision']=col[4]
    new_col['scale']=col[5]
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
        if args.srcdb_dict['type']=='vertica':
            col_dict=vertica2dict(args,col)
            cols.append(col_dict)
        elif args.srcdb_dict['type']=='oracle':
            col_dict=oracle2dict(args,col)
            cols.append(col_dict)
        elif args.srcdb_dict['type']=='db2':
            col_dict=db22dict(args,col)
            cols.append(col_dict)
        elif args.srcdb_dict['type']=='snowflake':
            col_dict=snow2dict(args,col)
            cols.append(col_dict)
        else:
            raise ValueError(f"Unsupported Database {args.srcdb_dict['type']}")
        #cols.append(','.join(map(str,col)))
            
    return cols


def write_ddl_info(args,fname,table,row_gen):
    fields=[]
    for firstrow in row_gen:
        fields=list(sorted(firstrow.keys()))
        ddl_rows=get_ddl(args,table)
        ddl_file=args.ddldir/f'{table}.csv'
        
        cursor_file=Path(args.ddldir/f'{table}_cursor.txt')
        cursor_file.write_text(str(args.con.cursor.description).replace('(','\n('))

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

        args.log.info(f'\t----starting dump_table for: {args.srcschema}.{table}')

        if fname.exists() and not args.rerun: 
            args.log.info(f'SKIPPING: already finished {args.srcschema}.{table} found {fname}')
            base_results[f'{prefix}warning']='skipped'
            base_results[f'{prefix}status']='skipped'
            return base_results

        args.srcdb_dict = dbsetup.Envs[args.srcenv][args.srckey]
        args.srcdb_dict['db'] = args.srcdb

        with  dblib.DB(args.srcdb_dict,log=args.log,port=args.srcdb_dict.get('port','')) as con:
            args.con=con
            if table_dict.get('sql'):
                sql=table_dict.get('sql').upper()
                sql=sql.replace(f"SELECT ",f"SELECT '{args.load_key}' as BWC_DW_LOAD_KEY, '{args.load_ts}' as BWC_DW_EXTRACT_TS, ")
                args.log.debug(f'sql override found: {sql}')
            else:
                cols=con.get_cols(args.srcschema,table)
                if not cols: raise ValueError(f'No columns for {args.srcschema}.{table}')

                cols_str=','.join(cols)
                sql=f"select  {cols_str},\'{args.load_key}\' as BWC_DW_LOAD_KEY, \'{args.load_ts}\' as BWC_DW_EXTRACT_TS  from {args.srcschema}.{table}"
                if args.limit_rows: sql+=' limit 10'

            count_sql= f''' with extract_query as ({sql}) select count(*) as COUNT from extract_query'''
            src_count=list(args.con.fetchdict(count_sql,size=1))[0]['COUNT']
            args.log.info(f'source table validation sql ={count_sql}')
            args.log.info(f'source table count ={src_count}')
            base_results[f'{prefix}src_count']=src_count

            if not src_count:
                base_results[f'{prefix}warning']='empty table'
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
                
                args.log.info(f'\t----Child done {args.srcschema}.{table} write_dict')
                return write_dict
            else:
                args.log.error(f'\t----Child done {args.srcschema}.{table}')
                raise AssertionError(f'NO DDL for table {args.srcschema}.{table}')
                return base_results

    except:
        errmsg=inf.geterr()
        print(errmsg)
        base_results[f'{prefix}error']=errmsg
        base_results[f'{prefix}status']='error'
        if args: args.log.error(errmsg)
        else: print(errmsg)
        args.log.info(f'\t----Child done {args.srcschema}.{table}')

    return base_results

# Parse the command line arguments
def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --srcdir /dev/cam/base",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='/env/database/schema/[table]')
    parser.add_argument( '--load_version',required=True,help='custom load file to use')
    #boolean
    parser.add_argument( '--raw', default=False,action='store_true',help='Just raw data dump by default: remove delimiter,term, and convert to ascii')
    parser.add_argument( '--rerun', default=False,action='store_true',help='Over write current data')
    parser.add_argument( '--ddl_only',default=False,action='store_true',help='Only extract DDLs, and changes rowcount for fetches to 1')
    parser.add_argument( '--full_schema',default=False,action='store_true',help='Extract all tables within the specified schema')
    parser.add_argument( '--limit_rows',default=False,action='store_true',help='Set row limit, default=10')
    parser.add_argument( '--varchar_id', default=False, action='store_true', help='Set columns ending in "_ID" to varchar datatype')
    #optional
    parser.add_argument( '--skip', default='',help='comma list of tables to skip')
    parser.add_argument( '--level', default=1,help='where in call chain this is for logging')
    parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
    parser.add_argument( '--delim', default='\t',help='delim to use, e.g. ~ | ')
    parser.add_argument( '--term', default='\n',help='line terminator to use, defaults to \n  newline')
    parser.add_argument( '--parallel', default=5,type=int,help='num of extracts to do in parallel, default= 1 single threaded mode ')
    parser.add_argument( '--logdir', default='',help='default logging directory, $root/env/conn/schema/load_key/logs')
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--table',help='specific table to extract when not doing full_schema, e.g. DATE_DIM')
    parser.add_argument( '--max_col',default=64000,type=int,help='Maxmimum column width, default=64000')   
    
    args = parser.parse_args()


    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key=ymd
    args.load_ts=ymd_hms

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srclog,args,srcenv
    #use_load_key=False, find_src_load_key=False 
    #alternative way to determine load file location
    inf.build_args_paths(args,use_load_key=True, find_src_load_key=True)
    args.prefix=''

    args.root=Path(__file__).resolve().parent.parent
    args.loaddir=args.root/'bwcloads'
    if args.load_version:
        args.load_file=args.loaddir/args.srcenv/args.srcdb/f'{args.srcschema}_{args.load_version}.csv'

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir=args.srclog
    args.ddldir=args.srcdata/'ddl'
    args.csvdir=args.srcdata/'extracts'

 # Directories will be created if they do not already exist.
    args.ddldir.mkdir(parents=True, exist_ok=True)
    args.csvdir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app='parent',prefix=args.level*'\t')
    args.log.info(f'{sys.argv[0]} launched with arguments: {sys.argv[1:]}')
    args.log.debug(f'Saving files to:{args.ddldir} {args.csvdir} {args.logdir}')
    args.log.debug(f'args global settings:{args}')

    return args

def get_tables(args):
    dbdict = dbsetup.Envs[args.srcenv][args.srckey]
    con=dblib.DB(dbdict,log=args.log,port=dbdict.get('port',''))
    tables=con.get_tables(args.srcschema)
    return tables

def build_all_extract_jobs(args):
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['table'].startswith('#'): continue
        yield source_dict




# Process and check to see if  full_schema has been chosen.  
# If not, then parse out the tables as a list, using comma as a separator
# Determine the Parallelism to use when running.  Default = 1


def process_results(args,begin,results):
    args.log.debug('producing reports')
    prefix=args.prefix
    end=datetime.datetime.now()
    first_error='';error_count=0
    empty_tables=[];processed_tables=[];error_tables=[];skipped_tables=[]
    for result in results:
        #args.log.debug(str(result))
        if not first_error:
            if result.get(f'{prefix}error'): first_error=result[f'{prefix}error'];error_count+=1
        result[f'{prefix}parent_start']=begin.strftime('%Y%m%d-%H:%M:%S')
        result[f'{prefix}parent_end']=end.strftime('%Y%m%d-%H:%M:%S')
        result[f'{prefix}parent_runtime']=round((end-begin).total_seconds(),2)

        if result[f'{prefix}error']:
                error_tables.append(result[f'{prefix}fname'])
        elif result[f'{prefix}rows']==0:
            if result[f'{prefix}warning']=='skipped':
                skipped_tables.append(result[f'{prefix}fname'])
            else:
                empty_tables.append(result[f'{prefix}fname'])
        else:
            processed_tables.append(result[f'{prefix}fname'])

    args.log.error(f'Error tables {len(error_tables)},{error_tables}')  
    args.log.info(f'Skipped tables {len(skipped_tables)},{skipped_tables}')  
    args.log.info(f'Processed tables {len(processed_tables)},{processed_tables}')   
    args.log.info(f'Empty tables {len(empty_tables)},{empty_tables}')   

    ymd_hms=args.now.strftime('%Y_%m_%dT%H%M%S') 
    report=f'{args.logdir}/report_{ymd_hms}_{Path(sys.argv[0]).name}.csv'
    inf.write_csv(report,results,log=args.log,delim=',')

    # -----
    if error_count:
        first_error=first_error.replace('\n','')
        args.log.error(f'-==# Processing Completed w\errors {first_error} #==-')
        raise AssertionError(f'found errors in children count:{error_count} first_error {first_error}')
    elif error_tables:
        args.log.error(f'Error tables {len(error_tables)},{error_tables}')  
        raise AssertionError(f'Could not extract tables {len(error_tables)},{error_tables}')
    else:
        args.log.error('-==# Processing is Complete #==-')



def main():
    try:
        args=None
        args=process_args()
        skip_tables={}
        if args.skip:
            skip_tables={atable.upper() for atable in args.skip.split(',')}

        if args.full_schema:
            tables = { table:{} for table in get_tables(args) if table.upper() not in skip_tables}
        elif not args.table:
            #tables=extract_info.Envs[args.srcenv][args.srcdb][args.srcschema]
            extract_rows=list(build_all_extract_jobs(args))
            tables={}
            for row in extract_rows:
                print(row['sql'])
                tables[row['table'].upper()]={'sql':row['sql']}

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

        process_results(args,args.now,results)
        args.log.info('done')
    
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
python run_extract.py --srcdir /dev/vertica_me/RUB1/A82581 --table TCDAPCI
python run_extract.py --srcdir /uat/oracle_etl/pub1/pcmp --load_version example

'''