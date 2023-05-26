import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
from pathlib import Path
from collections import OrderedDict

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
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

import threading, queue


def build_ddl(args,table,table_dict):
    #sql_option='VIEW IF NOT EXISTS'
    sql_option='OR REPLACE TABLE'

    sql = f"CREATE {sql_option} {args.tgtdb}.{args.tgtschema}.{table } ( \n     "

    rows=[]
    for col,dtype in table_dict.items():
        col = inf.rename_col(args,col)
        #if '$' in col or '#' in col: col=f'"{col}"'
        rows.append(f'{col} {dtype},')
    rows[-1]=rows[-1].strip(',')

    sql += '\n     '.join(rows)
    sql+='\n);'
    #print(sql);input('go')

    return sql

def create_ddl(args,src_table,tgt_table):
    auto_header=None

    if not args.header:
        args.log.info('using header in file')
    elif args.header.lower() == 'bwcstd':
        auto_header=inf.build_header(src_table,delim=args.file_delim)
    else:
        raise ValueError(f'unsupported {args.header}')

    rows=inf.read_csv(src_table,delim=args.file_delim,header=auto_header)
    for row in rows: break

    if args.tgtdb_dict['type']=='snowflake':
        fields={}
        for acol in row.keys():
            #acol="'"+acol+"'"
            fields[acol]='TEXT'
    else:
        raise ValueError('unsupported database')

    sql= build_ddl(args,tgt_table,fields)
    final_ddl_file=args.tgt_ddldir/(tgt_table+'_ddl.txt')
    final_ddl_file.write_text(sql)
    args.log.info(f'target ddl {tgt_table} saved to {final_ddl_file}')
    return sql


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
    args.log.info(copy_cmd)
    result=list(args.con.exe(copy_cmd))
    args.log.info(str(result))
    return result[0]

#------------------------------  View Layer
def create_views(args,src_table,tgt_view=''):
    junk,viewdb,viewschema=args.view.split('/')
    junk,presentdb,presentschema=args.present.split('/')

    #src_table_cols=set([col.upper() for col in args.con.get_cols(presentschema,table=src_table,db=presentdb)])
    src_table_cols = []
    coldict = {}

    for col in args.con.get_cols(presentschema,table=src_table,db=presentdb):
        dtype = coldict.get(col,'unknown')

        if dtype == 'date':
            src_table_cols.append(col.upper())
        elif dtype == 'integer':
            src_table_cols.append(col.upper())
        elif dtype == 'datetime':
            src_table_cols.append(col.upper())
        else:
            src_table_cols.append(col.upper())

    src_cols_str=','.join(src_table_cols)

    sql_option='VIEW IF NOT EXISTS'

    src_fullyqtable=f'{presentdb}.{presentschema}.{src_table}'
    tgt_fullyqview=f'{viewdb}.{viewschema}.{src_table}'

    args.con.exe(f"create SCHEMA IF NOT EXISTS  {viewdb}.{viewschema}")

    if args.con.dbtype=='snowflake':
        sql=f'''
        CREATE  {sql_option}  {tgt_fullyqview} AS (
            select {src_cols_str} from {src_fullyqtable}
        )
        '''
    else:
        raise ValueError(f'Database not supported {args.dbcon.dbtype}')

    
    return sql

#------------------------------  Presentation Layer
def create_present(args,src_table,tgt_view=''):
    junk,viewdb,viewschema=args.present.split('/')
    if not tgt_view: tgt_view=src_table


    if args.append:
        junk,srcdb,srcschema=args.append.split('/')
    elif args.replace:
        junk,srcdb,srcschema=args.replace.split('/')

    src_table_cols=list([col.upper() for col in args.con.get_cols(srcschema,table=src_table,db=srcdb)])
    src_cols_str=','.join(src_table_cols)

    sql_option='or REPLACE TABLE'
    #sql_option='TABLE IF NOT EXISTS'

    src_fullyqtable=f'{srcdb}.{srcschema}.{src_table}'
    tgt_fullyqview=f'{viewdb}.{viewschema}.{src_table}'

    args.con.exe(f"create SCHEMA IF NOT EXISTS  {viewdb}.{viewschema}")

    if args.con.dbtype=='snowflake':
        sql=f'''
        CREATE  {sql_option}  {tgt_fullyqview} AS (
        with add_id as ( 
            select NULL as BWC_ID,*
            from {src_fullyqtable}
        )
              select {src_cols_str},BWC_ID from add_id
        )
        '''

        #sql2=sql.replace('NULL as BWC_ID,',f"NULL as BWC_ID,'{sql}' as BWC_SQL")

    else:
        raise ValueError(f'Database not supported {args.dbcon.dbtype}')

    
    return sql

# ------------------------------------------------- Appending or replacing

def replace_table(args,table,replace_table=''):
    if not replace_table: replace_table=table

    replace=args.replace
    junk,replace_db,replace_schema=replace.split('/')
    args.con.exe(f"create SCHEMA IF NOT EXISTS  {replace_db}.{replace_schema}")
    args.con.exe(f"CREATE OR REPLACE TABLE  {replace_db}.{replace_schema}.{replace_table}  clone {args.tgtdb}.{args.tgtschema}.{table}")

def append_table(args,table,append_table=''):
    if not append_table: append_table=table
    junk,append_db,append_schema=args.append.split('/')

    tgt_table_cols=set([col.upper() for col in args.con.get_cols(args.tgtschema,table=table,db=args.tgtdb)])
    append_table_cols=set([col.upper() for col in args.con.get_cols(append_schema,table=append_table,db=append_db)])

    if not append_table_cols:
        args.log.info('No columns found, replacing table with new table')
        return 'need to run replace'
    else:
        new_cols_to_add=tgt_table_cols.difference(append_table_cols)
        removed_cols=set(append_table_cols).difference(tgt_table_cols)
        if new_cols_to_add: args.log.info(f'{new_cols_to_add} were added to source')
        if removed_cols: args.log.info(f'{removed_cols} were removed from source')

        fq_src=f"{args.tgtdb}.{args.tgtschema}.{table}"
        fq_tgt=f"{append_db}.{append_schema}.{append_table}"

        table_cols_str=','.join(tgt_table_cols)
        cols_type_to_add=[]
        for col in new_cols_to_add:
            col=col+' TEXT'
            cols_type_to_add.append(col)

        list_of_calls_to_add = ','.join(cols_type_to_add)

        if list_of_calls_to_add: 
            alter_sql = f"ALTER TABLE {fq_tgt} ADD {list_of_calls_to_add}"
            args.con.exe(alter_sql)
        insert_sql =f"INSERT INTO {fq_tgt} ({table_cols_str}) SELECT {table_cols_str} from {fq_src}"
        args.con.exe(insert_sql)


# -------------------------------------------------------  Snowflake staging
def get_fields(args,fname):
    fname=Path(fname)
    first_row=''
    if fname.suffix=='.gz':
        fr=gzip.open(fname)
        for row in fr:
            first_row=row.strip().decode()
            break
    else:
        fr=open(fname)
        for row in fr:
            first_row=row.strip()
            break

    return first_row.split(args.file_delim)


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

    fname=src_file
    fields=get_fields(args,fname)
    fields_str=','.join(fields)
        # Copy from local file

    stagedir=snow_get_stagedir(args.tgtdb,args.tgtschema,table)

    if snow_exists_stage(args,stagedir):
        result=snow_remove_stage(args,stagedir)
        print('removed',result,'files')

    if snow_exists_stage(args,stagedir):
        args.log.warn(f'Stage not empty')
        raise Warning(f'Stage directory: {stagedir}  not empty')

    snow_put(args,fname,stagedir)
    result = snow_copy_into(args,args.tgtdb,args.tgtschema,table,stagedir)

    #[{'file': 'DBTEST/x10057301/a/a.csv.gz', 'status': 'LOADED', 'rows_parsed': 3, 'rows_loaded': 3, 'error_limit': 3, 'errors_seen': 0, 'first_error': None, 'first_error_line': None, 'first_error_character': None, 'first_error_column_name': None}]

    tmp_loadfname.write_text(f'{begin} starting\n {time.asctime()} done\n')
    args.log.info(f'marking as done by replacing {tmp_loadfname} {fname} ')

    alter_load_key=f"alter table {args.tgtdb}.{args.tgtschema}.{table} add column BWC_DW_LOADKEY TEXT default '{args.load_key}'";
    args.con.exe(alter_load_key)
    alter_extract_ts=f"alter table {args.tgtdb}.{args.tgtschema}.{table} add column BWC_DW_EXTRACT_TS TEXT default '{args.load_ts}'";
    args.con.exe(alter_extract_ts)
    alter_row_parse=f"alter table {args.tgtdb}.{args.tgtschema}.{table} add column BWC_DW_ROW_PARSE INTEGER default {result['rows_parsed']}";
    args.con.exe(alter_row_parse)
    alter_row_load=f"alter table {args.tgtdb}.{args.tgtschema}.{table} add column BWC_DW_ROW_LOAD INTEGER default {result['rows_loaded']}";
    args.con.exe(alter_row_load)
    alter_error_seen=f"alter table {args.tgtdb}.{args.tgtschema}.{table} add column BWC_DW_ERROR_SEEN INTEGER default {result['errors_seen']}";
    args.con.exe(alter_error_seen)

    tmp_loadfname.replace(load_fname)

    # cur.execute("COPY table(field1, field2) FROM LOCAL"
    #             " 'data_Jan_*.csv','data_Feb_01.csv' DELIMITER '{delim}'"
    #             " REJECTED DATA 'path/to/write/rejects.txt'"
    #             " EXCEPTIONS 'path/to/write/exceptions.txt'",
    #             buffer_size=65536
    # )


def getcount(args,table):
    base_results={}
    count_sql= f'''select count(*) as COUNT from {args.tgtdb}.{args.tgtschema}.{table}'''
    print('******',list(args.con.fetchdict(count_sql,size=1)))
    tgt_count=list(args.con.fetchdict(count_sql,size=1))[0]['COUNT']
    args.log.info(f'tgt table count ={tgt_count}')
    return tgt_count

def load_table(args,tgt_table):
    prefix=''
    args.snow_prefix='@~'


    load_fname=Path(args.loaddir)/f'{tgt_table}.txt'
    base_results= {f'{prefix}count':0,f'{prefix}runtime':0,f'{prefix}table':tgt_table,f'{prefix}fname':load_fname,f'{prefix}rate':0,
                    f'{prefix}status':'',f'{prefix}runtime':0,f'{prefix}error':'',f'{prefix}warning':''}

    if load_fname.exists() and not args.rerun: 
        args.log.info(f'SKIPPING: already finished {args.tgtdb}.{args.tgtschema}.{tgt_table}')
        base_results[f'{prefix}warning']='skipped'
        base_results[f'{prefix}status']='skipped'
        return base_results

    #create table to match the data
    now=datetime.datetime.now()

    args.log.info,(f'creating tgt_table: {args.tgtdb}.{args.tgtschema}.{tgt_table}')

    if args.tgtdb_dict['type']=='snowflake':
        args.con.exe(f"create SCHEMA IF NOT EXISTS {args.tgtdb}.{args.tgtschema}")
        args.con.exe(f"drop TABLE IF EXISTS {args.tgtdb}.{args.tgtschema}.{tgt_table}")
        ddl_sql=create_ddl(args,args.srcfile,tgt_table)
        args.con.exe(ddl_sql)
    else:
        raise ValueError(f'unsupported database for loading {args.tgtdb}')


    #now load the data
    args.log.info,(f'loading: {args.tgtdb}.{args.tgtschema}.{tgt_table} into {args.tgtdb}')
    if args.tgtdb_dict['type']=='snowflake':
        load_data_snowflake(args,args.srcfile,tgt_table)
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


# ---- STANDARD setup Functions

def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    codedir=str(Path(f"{os.environ['USERPROFILE']}")/'bwcroot/bwcenv')
 
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
    #required
    parser.add_argument( '--srcfile', required=True,help='src file to load')
    parser.add_argument( '--tgtdir', required=True,help='data target /env/db/schema')
    #boolean
    #parser.add_argument( '--force', default=False,action='store_true',help='over write current data')
    #optional
    parser.add_argument( '--header', default='',help='bwcstd or custom header')
    parser.add_argument( '--append', default='',help='replace table in source layer')
    parser.add_argument( '--replace', default='',help='append table in source layer')
    parser.add_argument( '--present', default='',help='present table from the source layer')
    parser.add_argument( '--view', default='',help='create view if not there')
    parser.add_argument( '--eldir', default=eldir,help='default directory to use')
    parser.add_argument( '--table', default='',help='override table name to use')
    parser.add_argument( '--delim', default=r'\t',help='delim to use')
    parser.add_argument( '--term', default=r'\n',help='terminator to use')
    parser.add_argument( '--logdir', default='',help='logdir')
    parser.add_argument( '--load_key', default='',help='use a specific load key instead of most recent')
    parser.add_argument( '--cleanlog',action='store_true', help='changes the logger to overwrite mode')
    
    args = parser.parse_args()

    args.rerun=True
    now=datetime.datetime.now()
    args.now=now

    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    args.load_key=ymd
    args.load_ts=ymd_hms

    args.file_delim=args.delim
    if args.file_delim==r'\t':
        args.file_delim='\t'

    dest_final=view_final=source_final=''
    if  args.append.upper() =='BWCSTD' or  args.replace.upper() =='BWCSTD':
        dest=args.tgtdir.upper()
        #
        if '_STAGING' not in dest: raise ValueError(f'missing STAGING {args.tgtdir}')
        source_fields=dest.replace('_STAGING','_SOURCE').split('/')[-2:]
        source_final='/'+'/'.join(source_fields)
        if args.append: args.append=source_final
        if args.replace: args.replace=source_final
        #
        present_fields=dest.replace('_STAGING','_PRESENT').split('/')[-2:]
        present_final='/'+'/'.join(present_fields)
        args.present=present_final
        #
        view_fields=dest.replace('_STAGING','_VIEWS').split('/')[-2:]
        view_final='/'+'/'.join(view_fields)
        args.view=view_final

        dest_final=f'bwcstd option set: {source_final} {present_final} {view_final}'


    #NOTE:switches tgtdir to an actual filepath
    inf.build_args_paths(args,use_load_key=False, find_src_load_key=False)

    args.logdir=args.tgtlog
    args.srcfile=Path(args.srcfile)

    args.root=Path(__file__).resolve().parent.parent

    #tgt table
    #args.tgt_table=args.srcfile.stem
    args.tgt_table=args.srcfile.stem.split('.',1)[0]

    if args.table: args.tgt_table=args.table

    #target dirs
    args.loaddir=args.tgtdata/'loads'
    args.tgt_ddldir=args.tgtdata/'ddl'
    args.tgt_ddldir.mkdir(parents=True, exist_ok=True)

    args.logdir.mkdir(parents=True, exist_ok=True)
    args.loaddir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app='parent',cleanlog=args.cleanlog)
    args.log.debug('Running on:'+os.environ['COMPUTERNAME'])
    if dest_final: args.log.info(f'{dest_final}')
    args.log.info(f'processing from:{args.tgtdir}')
    return args

def save_results(args,base_results):
    base_results_list=[base_results]
    now=datetime.datetime.now()
    ymd_hms=now.strftime('%Y_%m_%dT%H%M%S') 
    report=f'{args.logdir}/report_{ymd_hms}_{Path(sys.argv[0]).name}.csv'
    inf.write_csv(report,base_results_list,log=args.log,delim=',')

def main():
    try:
        args=None
        begin=datetime.datetime.now()
        args=process_args()
    

        args.log.info(f'file to be loaded:{args.srcfile}')

        #get conn info
        args.tgtdb_dict = dbsetup.Envs[args.tgtenv][args.tgtkey]
        args.tgtdb_dict['db'] = args.tgtdb
        args.log.info(f'using database info for:{args.tgtenv} {args.tgtdb}')

        #connect and process
        with  dblib.DB(args.tgtdb_dict,log=args.log,port=args.tgtdb_dict.get('port','')) as args.con:

            #stage layer
            load_results=load_table(args,args.tgt_table)

            #source layer
            append_result=''
            if args.append:
                args.append=args.append.upper()
                append_result=append_table(args,args.tgt_table)

            #switch from append to replace for the first time
            if append_result=='need to run replace':
                args.replace=args.append
                args.log.info('table needs to be replaced, set replace option')

            if args.replace:
                replace_table(args,args.tgt_table)

            #present layer
            if args.present:
                sql=create_present(args,args.tgt_table)
                args.con.exe(sql)

            #view layer
            if args.view:
                sql=create_views(args,args.tgt_table)
                args.con.exe(sql)

            #logging
            save_results(args,load_results)
            args.log.info('-==# Processing is Complete #==-')
    except:
        if args:
            args.log.info(inf.geterr())
            raise
        else:
            print(inf.geterr())
            raise



if __name__=='__main__':
    main()

r'''
MANUAL loading
python run_fileload.py --srcfile E:\Temp\Testing\ed.csv --tgtdir /dev/snow_bi/DBTEST/x10057301  --replace /DBTEST/PUBLIC --delim ,

bwcstd Automatic loading
python run_fileload.py --srcfile E:\Temp\Testing\ed.csv --tgtdir /dev/snow_bi/DBTEST/DBTEST_STAGING  --replace bwcstd --delim ,
python run_fileload.py --srcfile E:\Temp\Testing\ed.csv --tgtdir /dev/snow_bi/DBTEST/DBTEST_STAGING  --replace append --delim ,
python run_fileload.py --srcfile E:\Temp\Testing\ed.csv --tgtdir /uat/snow_etl/UAT_STAGING/BWCMISC --append bwcstd --delim ,
python run_fileload.py --srcfile E:\Temp\Testing\IC_HEARING_NOTICES.txt --tgtdir /uat/snow_etl/UAT_STAGING/BWCMISC --append bwcstd --delim |

python run_fileload.py --srcfile I:\IT\ETL\TRANSFER\PD\MCO_Imaging\TBatch.txt.gz                 --tgtdir /uat/snow_etl/UAT_STAGING/BWCMISC --replace bwcstd --delim \t --header bwcstd --eldir c:/temp
python run_fileload.py --srcfile I:\IT\ETL\TRANSFER\PD\MCO_Imaging\TIndex.txt.gz                 --tgtdir /uat/snow_etl/UAT_STAGING/BWCMISC --replace bwcstd --delim \t --header bwcstd --eldir c:/temp
python run_fileload.py --srcfile I:\IT\ETL\TRANSFER\PD\MCO_Imaging\MCO_DCMNT_TRANSMISSION.txt.gz --tgtdir /uat/snow_etl/UAT_STAGING/BWCMISC --replace bwcstd --delim \t --header bwcstd --eldir c:/temp
python run_fileload.py --srcfile I:\IT\ETL\TRANSFER\PD\Policy_File_Inventory/Policies_Merge.csv  --tgtdir /uat/snow_etl/UAT_STAGING/BWC_ETL --replace bwcstd --delim ,  --header bwcstd --eldir c:/temp --table DW_POLICY_FILE_INVENTORY_HISTORY_tmp


'''
