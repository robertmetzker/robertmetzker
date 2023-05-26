
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
from pathlib import Path


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

#from bwcenv.bwclib import dblib,inf
from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

'''
col order does not matter
schema created at runtime
'''

import threading, queue
   


def convert_to_snowflake(args,table_rows):


    table_dict={}

    for row_dict in table_rows:
        name=row_dict['name'];prec=row_dict['precision'].strip();scale=row_dict['scale'].strip();size=row_dict['display_size'];datatype=row_dict['datatype']
        if not prec: prec=0
        if not scale: scale=0

        prec=int(prec);absprec=abs(prec);scale=int(scale);absscale=abs(scale)
        
        if datatype=='NUMBER': 
            if absscale>15: val='float' 
            #elif prec==0 and prec<scale and scale!=0:  val='NUMERIC(36,6)' #increasingly expensive for p=37, 58, 67, etc., where p <= 1024
            elif prec==0 and scale<=0: val='INTEGER'
            
            elif absprec>38: val=f"NUMERIC(38,{scale})"
            
            else:
                val=f"NUMERIC({prec},{scale})"
        elif datatype=='TIMESTAMP':
            val=f'TIMESTAMP'
        elif datatype=='DATE':
            val=f'DATE'
        else:
            val=f'TEXT'

        table_dict[name]=val

    #table_dict['BWC_DW_LOAD_START_DTTM']='TIMESTAMP DEFAULT current_timestamp'

    return table_dict


def convert_to_oracle(args,table_rows):


    table_dict={}

    for row_dict in table_rows[:2]:
        name=row_dict['name']
        prec=row_dict['precision'].strip();scale=row_dict['scale'].strip();size=row_dict['display_size'];datatype=row_dict['datatype']
        if not prec: prec=0
        if not scale: scale=0

        prec=int(prec);absprec=abs(prec);scale=int(scale);absscale=abs(scale)
        
        if datatype=='NUMBER': 
            if absscale>15: val='float' 
            #elif prec==0 and prec<scale and scale!=0:  val='NUMERIC(36,6)' #increasingly expensive for p=37, 58, 67, etc., where p <= 1024
            elif prec==0 and scale<=0: val='NUMBER(38)'
            
            elif absprec>38: val=f"NUMERIC(38,{scale})"
            
            else:
                val=f"NUMERIC({prec},{scale})"
        elif datatype=='TIMESTAMP':
            val=f'TIMESTAMP'
        elif datatype=='DATE':
            val=f'DATE'
        else:
            val=f'VARCHAR2(4000)'

        #table_dict[name]=val
        table_dict[name]=f'VARCHAR2(4000)'

    return table_dict


def convert_types(args,table,table_info):
    '''
    tgtdb_dict={'server': 'dza60922.us-east-2.aws', 'db': 'DBTEST', 'login': 'X10111831', 'passwd': 'XXXX', 'type': 'snowflake', 'warehouse': 'WH_BI'},
    '''

    #convert to target data types
    if args.tgtdb_dict['type']=='snowflake':
        final_dict=convert_to_snowflake(args,table_info)
    elif args.tgtdb_dict['type']=='oracle':
        final_dict=convert_to_oracle(args,table_info)
    else:
        raise ValueError('unsupported database')

    return final_dict


def build_ddl(args,table,table_dict):
    if args.tgtdb_dict['type']=='snowflake':
        sql = f"CREATE TABLE IF NOT EXISTS {args.tgtdb}.{args.tgtschema}.{table } ( \n     "

    elif args.tgtdb_dict['type']=='oracle':
        sql = f"CREATE TABLE {args.tgtschema}.{table } ( \n     "

    rows=[]
    for col,dtype in sorted(table_dict.items()):
        if '$' in col or '#' in col: col=f'"{col}"'
        rows.append(f'{col} {dtype},')
    rows[-1]=rows[-1].strip(',')

    sql += '\n     '.join(rows)
    sql+='\n);'

    return sql

def get_fields(fname):
    fr=gzip.open(fname)

    first_row=''
    for row in fr:
        first_row=row.strip().decode("ascii")
        break
    
    return first_row.split('\t')

def create_ddl(args,src_table,tgt_table):
    
    ddl_file=args.ddldir/(src_table+'.csv')
    args.log.info(f'creating ddl for {src_table} using {ddl_file}')
    table_info=list(inf.read_csv(ddl_file,'\t'))

    final_ddl_file=args.tgt_ddldir/(tgt_table+'_ddl.txt')
    
    table_dict=convert_types(args,src_table,table_info)
    sql= build_ddl(args,tgt_table,table_dict)
    final_ddl_file.write_text(sql)
    args.log.info(f'target ddl {tgt_table} saved to {final_ddl_file}')
    return sql

#------------------------------------Oracle Loading------------------------------------



#------------------------------------SNOWFLAKE FUNCTIONS------------------------------------
def snow_exists_stage(args,stagedir):
    '''
        {'name': '@~/NTWK.csv.gz'}
    '''
    for row in snow_list_stage(args,stagedir):
        if row['name']:
            result=('STOPPING:STAGE:found',row['name'])
            return result
    return False

def snow_remove_stage(args,stagedir):
    '''
    rm @~/DBTEST/DBT_PBALZER/ACTIVITY_NAME_TYPE/ACTIVITY_NAME_TYPE.csv.gz;
    '''
    cmd_list=[]
    for row in snow_list_stage(args,stagedir):
        remove_cmd=f"""rm @~/{row['name']}; """
        args.con.exe(remove_cmd)
        cmd_list.append(remove_cmd)
    args.log.info(f'Deleted {len(cmd_list)} files from {stagedir} : {cmd_list}')
    return cmd_list

def snow_get_stagedir(dbname,schema,table):
    '''
    https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage.html
    @~ character combination identifies a user stage, @% character combination identifies a table stage.
    examples: '@~/path 1/file 1.csv'   '@%my table/path 1/file 1.csv'  '@my stage/path 1/file 1.csv

    bwc example: @~/DBTEST/X10057301/test/
    '''
    stagedir=f"{dbname}/{schema}/"+str(table)+'/'
    print(stagedir)
    return stagedir

def snow_put(args,path,stagedir):
    '''
        requires the full path
    put file://I:\IT\ETL\nielsenjf\snowflake\extracts_active\ADR_TYP_INFSPLIT_2700000.gz @~/DBTEST/X10057301/ADR_TYP/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';
    '''

    stage_cmd=f'''put file://{path} {args.snow_prefix}/{stagedir} auto_compress=true;'''
    print(stage_cmd)
    result=args.con.exe(stage_cmd)
    staged_files=snow_list_stage(args,stagedir)
    
    if not staged_files:
        raise Warning(f'Missing staged file: {path}')
    args.log.info(f'Staged {staged_files}')
    return staged_files


def snow_list_stage(args,stagedir):
    '''
        https://docs.snowflake.com/en/sql-reference/sql/list.html

    list @~/DBTEST/DBT_PBALZER/ACTIVITY_NAME_TYPE/;
    {'name': 'DEV_SOURCE/BASE/CARE824/CARE824.csv.gz', 'size': 61953552, 'md5': 'e59bcf86c48aad5c366bce6c4e6409d1', 'last_modified': 'Mon, 4 Oct 2021 19:56:44 GMT'}
    '''
    sql=f'list {args.snow_prefix}/{stagedir}; '

    stage_files=[row for  row in args.con.fetchdict(sql) ]
    return stage_files


def snow_copy_into(args,dbname,schema,table,stagedir):
    file_format=f"""file_format =  (type = csv field_delimiter = '{args.delim}' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  """
    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""

    copy_cmd=f"""copy into {dbname}.{schema}.{table} from {args.snow_prefix}/{stagedir} {file_format} on_error='continue'; """
    '''[{'file': 'UAT_STAGING/BASE/CLM/CLM.csv.gz', 'status': 'LOAD_FAILED', 'rows_parsed': 30639, 'rows_loaded': 0, 'error_limit': 30639, 'errors_seen': 30639, 'first_error': "Date 'AMANDA' is not recognized", 'first_error_line': 2, 'first_error_character': 33, 'first_error_column_name': '"CLM"["INC_DTE":7]'}]'''
    result=list(args.con.exe(copy_cmd))
    status=result[0]['status']
    if status!='LOADED':
        raise ValueError(f'Bad status: {result}')
    args.log.info(result)


def load_data_oracle(args,src_file,table):
    begin=time.asctime()
    load_fname=Path(args.loaddir)/f'{table}.txt'
    tmp_loadfname=Path(args.loaddir)/f'{table}.txt.tmp'
    tmp_loadfname.write_text(f'{begin} starting\n')

    fname=args.csvdir/(src_file+'.csv.gz')
    #sql loading
    delim=args.delim
    if delim==r'\t': delim='\t'
    csv_reader=inf.read_csv(fname,delim=delim)
    args.con.insert_many(args.tgtdb,args.tgtschema,table,csv_reader)
    #
    tmp_loadfname.write_text(f'{begin} starting\n {time.asctime()} done\n')
    args.log.info(f'marking as done by replacing {tmp_loadfname} {fname} ')
    tmp_loadfname.replace(load_fname)

def load_data_snowflake(args,src_file,table):
    '''
        # Copy from local file
    cur.execute("COPY table(field1, field2) FROM LOCAL"
                " 'data_Jan_*.csv','data_Feb_01.csv' DELIMITER ','"
                " REJECTED DATA 'path/to/write/rejects.txt'"
                " EXCEPTIONS 'path/to/write/exceptions.txt'",
                buffer_size=65536
    )
    print("Rows loaded:", cur.fetchall())

    '''
    begin=time.asctime()
    load_fname=Path(args.loaddir)/f'{table}.txt'
    tmp_loadfname=Path(args.loaddir)/f'{table}.txt.tmp'
    tmp_loadfname.write_text(f'{begin} starting\n')

    fname=args.csvdir/(src_file+'.csv.gz')
    fields=get_fields(fname)
    fields_str=','.join(fields)
        # Copy from local file


    # delim=args.delim
    # if delim==r'\t': delim='\t'
    # csv_reader=inf.read_csv(fname,delim=delim)
    # args.con.insert_many(args.tgtdb,args.tgtschema,table,csv_reader)

    # input('go')
    stagedir=snow_get_stagedir(args.tgtdb,args.tgtschema,table)

    if snow_exists_stage(args,stagedir):
        result=snow_remove_stage(args,stagedir)
        print('removed',result,'files')

    if snow_exists_stage(args,stagedir):
        args.log.warn(f'Stage not empty')
        raise Warning(f'Stage directory: {stagedir}  not empty')

    snow_put(args,fname,stagedir)
    snow_copy_into(args,args.tgtdb,args.tgtschema,table,stagedir)
    tmp_loadfname.write_text(f'{begin} starting\n {time.asctime()} done\n')
    args.log.info(f'marking as done by replacing {tmp_loadfname} {fname} ')
    tmp_loadfname.replace(load_fname)

def getcount(args,table):
    base_results={}
    if  args.tgtdb_dict['type']=='snowflake':
        count_sql= f'''select count(*) as COUNT from {args.tgtdb}.{args.tgtschema}.{table}'''
    elif args.tgtdb_dict['type']=='oracle':
        count_sql= f'''select count(*) as COUNT from {args.tgtschema}.{table}'''
    else:
        raise ValueError(f'unsupported database for loading {args.tgtdb}')

    tgt_count=list(args.con.fetchdict(count_sql,size=1))[0]['COUNT']
    args.log.info(f'tgt table count ={tgt_count}')
    return tgt_count

def get_load_version_tables(args):
    args.log.info(f'Using load file {args.load_file}')
    source_tables=[]
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['table'].startswith('#'): continue
        source_tables.append(source_dict['table'])

    return source_tables

def load_table(allargs):
    args,src_table,tables_dict=allargs
    args.snow_prefix='@~'
    table_prefix=''
    if args.table_prefix: table_prefix=args.table_prefix+'_'
    tgt_table=table_prefix+src_table
    # if args.tablekey: tablekey= table+'_'+ args.load_key.strip()
    prefix=args.prefix
    status=''
    args.log=inf.setup_log(args.logdir,app=f'child_{tgt_table}',prefix=(args.level+1)*'\t',cleanlog=args.cleanlog )

    try:
        now=datetime.datetime.now()
        load_fname=Path(args.loaddir)/f'{tgt_table}.txt'
        base_results= {f'{prefix}count':0,f'{prefix}runtime':0,f'{prefix}table':tgt_table,f'{prefix}fname':load_fname,f'{prefix}rate':0,
                        f'{prefix}status':'',f'{prefix}runtime':0,f'{prefix}error':'',f'{prefix}warning':''}

        if load_fname.exists() and not args.rerun: 
            args.log.info(f'SKIPPING: already finished {args.tgtdb}.{args.tgtschema}.{tgt_table}')
            base_results[f'{prefix}warning']='skipped'
            base_results[f'{prefix}status']='skipped'
            return base_results

        #create table to match the data
        con=''
        args.tgtdb_dict = dbsetup.Envs[args.tgtenv][args.tgtkey]
        args.tgtdb_dict['db'] = args.tgtdb
        args.log.info(f'using database info for:{args.tgtenv} {args.tgtdb}')
        with  dblib.DB(args.tgtdb_dict,log=args.log,port=args.tgtdb_dict.get('port','')) as args.con:

            args.log.info,(f'creating tgt_table: {args.tgtdb}.{args.tgtschema}.{tgt_table}')

            if  args.tgtdb_dict['type']=='snowflake':
                args.con.exe(f"create SCHEMA IF NOT EXISTS {args.tgtdb}.{args.tgtschema}")
                args.con.exe(f"drop TABLE IF EXISTS {args.tgtdb}.{args.tgtschema}.{tgt_table}")
                ddl_sql=create_ddl(args,src_table,tgt_table)
                args.log.info(ddl_sql)
                args.con.exe(ddl_sql)
            elif  args.tgtdb_dict['type']=='oracle':
                # they do not want DW changing things in oracle
                #args.con.exe(f"create SCHEMA IF NOT EXISTS {args.tgtdb}.{args.tgtschema}")
                #args.con.exe(f"drop TABLE IF EXISTS {args.tgtdb}.{args.tgtschema}.{tgt_table}")
                #ddl_sql=create_ddl(args,src_table,tgt_table)
                #args.con.exe(ddl_sql)
                args.con.exe(f"truncate table {args.tgtschema}.{tgt_table}")
                args.log.info('DDL EXECUTED by DBA ahead of time')
            else:
                raise ValueError(f'unsupported database for loading {args.tgtdb}')

            #now load the data
            args.log.info,(f'loading: {args.tgtdb}.{args.tgtschema}.{tgt_table} into {args.tgtdb}')
            if args.tgtdb_dict['type']=='snowflake':
                load_data_snowflake(args,src_table,tgt_table)
                tgt_count=getcount(args,tgt_table)
                status='success'
            elif args.tgtdb_dict['type']=='oracle':
                load_data_oracle(args,src_table,tgt_table)
                tgt_count=getcount(args,tgt_table)
                status='success'
            else:
                raise ValueError(f'unsupported database for loading {args.tgtdb}')
            rate=0
            end=datetime.datetime.now()
            runtime=end-now
            runtime=(end-now).total_seconds()
            if runtime: rate=int(tgt_count/runtime)
            base_results[f'{prefix}count']=tgt_count;base_results[f'{prefix}runtime']=round(runtime,2)
            base_results[f'{prefix}rate']=rate;base_results[f'{prefix}status']=status
            return base_results
    except:
        errmsg=inf.geterr()
        args.log.error(errmsg)
        base_results[f'{prefix}error']=errmsg
        base_results[f'{prefix}status']='error'

    return base_results

def build_all_extract_jobs(args):
    rows=[]
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['table'].startswith('#'): continue
        rows.append(source_dict)
    return rows

def process_results(args,begin,results):
    prefix=args.prefix
    args.log.debug('producing reports')
    now=datetime.datetime.now()
    first_error='';error_count=0
    empty_tables=[];processed_tables=[];error_tables=[];skipped_tables=[]
    for result in results:
        #args.log.debug(str(result))
        if not first_error:
            if result.get(f'{prefix}error'): 
                first_error=result[f'{prefix}error'];error_count+=1
        result[f'{prefix}parent_start']=begin.strftime('%Y%m%d-%H:%M:%S')
        result[f'{prefix}parent_end']=now.strftime('%Y%m%d-%H:%M:%S')
        result[f'{prefix}parent_runtime']=round((now-begin).total_seconds(),2)

        if result[f'{prefix}error']:
                error_tables.append(result[f'{prefix}fname'])
        elif result[f'{prefix}count']==0:
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

    ymd_hms=now.strftime('%Y_%m_%dT%H%M%S') 
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




def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    codedir=str(Path(f"{os.environ['USERPROFILE']}")/'bwcroot/bwcenv')
 
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='data source /env/db/schema')
    parser.add_argument( '--tgtdir', required=True,help='data target /env/db/schema')
    parser.add_argument( '--load_version',required=True,help='custom load file to use')
    #boolean
    parser.add_argument( '--raw', default=False,action='store_true',help='just raw data dump by default: remove delimiter,term, and convert to ascii')
    parser.add_argument( '--rerun', default=False,action='store_true',help='over write current data')
    #optional
    parser.add_argument( '--level', default=1,help='level at which called')
    parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
    parser.add_argument( '--table', default='',help='table to use, if not specified all tables in extracts are used')
    parser.add_argument( '--delim', default=r'\t',help='delim to use')
    parser.add_argument( '--term', default=r'\n',help='terminator to use')
    parser.add_argument( '--parallel', default=5,type=int,help='num of extracts to do in parallel, None = py default, 1 = single threaded mode')
    parser.add_argument( '--logdir', default='',help='logdir')
    parser.add_argument( '--load_key', default='',help='use a specific load key instead of most recent')
    parser.add_argument( '--suffix', default='.gz',help='extension on extract files')
    parser.add_argument( '--table_prefix', default='',help='add prefix to the filenames. ex: bwcstage')
    parser.add_argument( '--sql',help='sql override')
    parser.add_argument( '--cleanlog',action='store_true', help='changes the logger to overwrite mode')
    
    
    args = parser.parse_args()
    if args.eldir=='dev': 
        args.eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    now=datetime.datetime.now()
    args.now=now
    args.prefix='ld_'

    inf.build_args_paths(args,use_load_key=True, find_src_load_key=True)
    args.logdir=args.tgtlog
    args.ddldir=args.srcdata/'ddl'
    args.csvdir=args.srcdata/'extracts'

    ymd=now.strftime('%Y_%m_%d') #2021_05_14

    args.root=Path(__file__).resolve().parent.parent
   
    if args.load_version:
        args.load_file=args.root/'bwcloads'/args.srcenv/args.srcdb/f'{args.srcschema}_{args.load_version}.csv'
    
    #target dirs
    args.loaddir=args.tgtdata/'loads'
    args.tgt_ddldir=args.tgtdata/'ddl'

    args.tgt_ddldir.mkdir(parents=True, exist_ok=True)
    args.ddldir.mkdir(parents=True, exist_ok=True)
    #args.csvdir.mkdir(parents=True, exist_ok=True)  #should already be extracted to
    args.logdir.mkdir(parents=True, exist_ok=True)
    args.loaddir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app=f'parent-{args.load_version}',prefix=args.level*'\t',cleanlog=args.cleanlog)
    args.log.info(f'processing from:{args.csvdir}')
    return args

def main():
    try:
        args=None
        begin=datetime.datetime.now()
        args=process_args()
        
        if not args.table:
            #tables=[ afile.name.split('.')[0] for afile in args.csvdir.iterdir() if afile.suffix==args.suffix]
            load_version_tables=get_load_version_tables(args)
            tables = load_version_tables
            extract_rows=build_all_extract_jobs(args)

            tables_dict={}
            for row in extract_rows:
                tables_dict[row['table'].upper()]=row

        else:
            tables=args.table.split(',')

            tables_dict={}
            for table in tables:
                tables_dict[table]={}


        if len(tables)<args.parallel: 
            args.log.debug(f'dropping parallel to {len(tables)}')
            args.parallel=len(tables)

        args.log.info(f'tables to be processed:{tables}')
        args.log.info(f'processing tables {len(tables)} parallel={args.parallel}')
        all_args=[(args,table,tables_dict) for table in tables]

        if args.parallel==1: 
            results=[]
            args.log.info('running in single threaded mode')
            for allarg in all_args: 
                result=load_table(allarg);results.append(result)
        else:
            args.log.info('running in parallel mode')
            results=inf.run_parallel(load_table,args=all_args,parallel=args.parallel)
        process_results(args,begin,results)
        runtime =datetime.datetime.now() - args.now
        args.log.info(f'RUN TIME = {runtime}')
        args.log.info('-==# END LOG #==-')
 
    
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
Latest Example:

**********Added for SQL_Server
python run_load.py --srcdir /uat/sql_etl_dep/DEP/dbo --tgtdir /uat/snow_etl/UAT_STAGING/DEP --load_version dim --parallel 15

python run_extract.py --srcdir /uat/oracle_etl/pub1/PCMP --load_version dim --parallel 15
python run_load.py --srcdir /uat/oracle_etl/pub1/PCMP --tgtdir /uat/snow_etl/UAT_STAGING/PCMP --load_version dim --parallel 15
python run_create_source_snowflake.py --srcdir /uat/oracle_etl/pub1/PCMP --tgtdir /uat/snow_etl/UAT_SOURCE/PCMP --load_version dim --snow_srcdb UAT_STAGING
python run_createviews_snowflake.py --srcdir /uat/oracle_etl/pub1/PCMP --snow_srcdb UAT_SOURCE --tgtdir /uat/snow_etl_prod/PREPROD_EDW/PCMP --load_version dim 


python run_load.py --srcdir /dev/vertica_etl/rpd1/dw_report --tgtdir /dev/oracle_etl/pda1/bwc-etl --load_version none --table claim_policy_history --eldir c:/temp

python run_load.py --parallel 1 --srcdir /dev/vertica_etl/rpd1/dw_report --tgtdir /dev/oracle_etl/pda1/bwc-etl --table dw_claim_policy_history --load_version none --eldir c:/temp/


python run_load.py --parallel 1 --srcdir /dev/vertica_etl/rpd1/dw_report --tgtdir /dev/oracle_etl/pda1/bwc_etl --table dw_claim_policy_history --load_version none --eldir c:/temp/

'''
    