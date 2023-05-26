import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime,shutil
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


def build_ddl(args,db,schema,table,table_dict):
    #sql_option='VIEW IF NOT EXISTS'
    sql_option='OR REPLACE TABLE'

    sql = f"CREATE {sql_option} {db}.{schema}.{table } ( \n     "

    rows=[]
    for col,dtype in table_dict.items():
        col = inf.rename_col(args,col)
        #if '$' in col or '#' in col: col=f'"{col}"'
        rows.append(f'{col} {dtype},')
    rows[-1]=rows[-1].strip(',')

    sql += '\n     '.join(rows)
    sql+='\n);'


    return sql

def create_ddl(args,db,schema,src_table,tgt_table,file_delim,file_term,header):

    table_cols=table_dtypes=None
    if args.header_dict:
        table_cols=args.header_dict.get(tgt_table)
    if table_cols:
        table_dtypes=args.dtypes_dict.get(tgt_table)

    auto_header=None

    std_delim = std_term = False

    if file_delim==r'\t' or len(file_delim)<=1: 
        std_delim=True

    if file_term==r'\n' or len(file_term)<=1: 
        std_term=True

    if header=='y':
        args.log.info('using header in file')
    else:
        if std_delim and std_term:
            auto_header=inf.build_header(src_table,delim=file_delim)
            args.log.info(f'using header {auto_header}')


    if std_delim and std_term:
        args.log.info(f'reading  std csv {src_table}')
        rows=inf.read_csv(src_table,delim=file_delim,header=auto_header,log=args.log)
    else:
        args.log.info(f'reading  complex csv {src_table}')
        rows=inf.read_complex_csv(src_table,term=file_term,delim=file_delim,new_term='\n',new_delim='\t')

    for row in rows: break
    args.log.info(f'first row {row}')

    if header=='n' and table_cols:
        columns=table_cols.split(',')
    else:
        columns=row.keys()


    if args.tgtdb_dict['type']=='snowflake':
        fields={}
        for acol in columns:
            #acol="'"+acol+"'"
            fields[acol]='TEXT'
            if table_dtypes:
                fields[acol]=table_dtypes.get(acol,'TEXT')

    else:
        raise ValueError('unsupported database')

    sql= build_ddl(args,db,schema,tgt_table,fields)
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
        remove_cmd=f"""rm '@~/{row["name"]}'; """
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

    path=str(path).replace('\\','/')
    stage_cmd=f'''put 'file://{path}' {args.snow_prefix}/{stagedir} auto_compress=true;'''

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


def snow_copy_into(args,src_file,dbname,schema,table,stagedir,delim,term,header):
    '''
     COPY INTO t1 (c1) FROM (SELECT d.$1 FROM @mystage/file1.csv.gz d);).

    COPY INTO db.schema.table(c1, c2, c3, c4) FROM (SELECT $1, $2, $3, $4 FROM '@stage/path/afile.csv.gz')
    '''

    table_cols=table_dtypes=None
    if args.header_dict:
        table_cols=args.header_dict.get(table)
    if table_cols:
        table_dtypes=args.dtypes_dict.get(table)

    skip_header=f'skip_header = 0'
    if header=='y': skip_header=f'skip_header = 1'
    elif header =='skip':
        skip_header=f'skip_header = 1'
        table_cols=''

    rec_term=f"RECORD_DELIMITER ='{term}'"
    if term==r'\n':rec_term=''
    final_file=src_file.name
    if not src_file.name.endswith('.gz'):
        final_file=f'{src_file.name}.gz'
    #file_format=f"""file_format =  (type = csv field_delimiter = '{delim}' {rec_term} {skip_header} FIELD_OPTIONALLY_ENCLOSED_BY = '"' ) FILES=('{final_file}') """
    file_format=f"""file_format =  (type = csv field_delimiter = '{delim}' {rec_term} {skip_header} FIELD_OPTIONALLY_ENCLOSED_BY = NONE REPLACE_INVALID_CHARACTERS = TRUE ) FILES=('{final_file}') """

    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""

    copy_cmd=f"""copy into {dbname}.{schema}.{table} from {args.snow_prefix}/{stagedir} {file_format} on_error='continue'; """

    if table_cols:
        select_cols = [f'${i+1}' for i in range(len(table_cols.split(','))) ]
        select_cols_str=','.join(select_cols)
        select_str=f'SELECT {select_cols_str}'
        copy_cmd=f"""copy into {dbname}.{schema}.{table}({table_cols}) FROM ({select_str} from {args.snow_prefix}/{stagedir}) {file_format} on_error='continue'; """
    args.log.info(copy_cmd)
    result=list(args.con.exe(copy_cmd))[0]
    if result['status']!='LOADED':
        raise ValueError(f'Problem loading table {dbname}.{schema}.{table} : {result}')
    args.log.info(str(result))
    return result
'''
snow_copy_into: (239): [{'file': 'uat_STAGING/DW_EXTERNAL_RISK_CONTROL/TIMEENTRYACTIVITIES/TimeEntryActivities.txt.gz', 'status': 'PARTIALLY_LOADED', 'rows_parsed': 584752, 'rows_loaded': 584699, 'error_limit': 584752, 'errors_seen': 53, 'first_error': "Invalid UTF8 detected in string '0xFFwebinar: Building a Complete Arc Flash Safety Program with NFPA 70E\r\n'", 'first_error_line': 1, 'first_error_character': 436, 'first_error_column_name': '"TIMEENTRYACTIVITIES"["COMMENT":6]'}]
'''
#------------------------------  View Layer
def create_views(args,src_table,tgt_view=''):
    args.log.info('AT CREATE VIEWS')

    #src_table_cols=set([col.upper() for col in args.con.get_cols(presentschema,table=src_table,db=presentdb)])
    src_table_cols = []
    coldict = {}

    for col in args.con.get_cols(args.tgtschema,table=src_table,db=args.presentdb):
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
    if args.force:
        sql_option='OR REPLACE VIEW'

    presentdb_fullyqtable=f'{args.presentdb}.{args.tgtschema}.{src_table}'
    viewdb_fullyqview=f'{args.viewdb}.{args.tgtschema}.{src_table}'

    args.con.exe(f"create SCHEMA IF NOT EXISTS  {args.viewdb}.{args.tgtschema}")

    if args.con.dbtype=='snowflake':
        sql1=f'''
        CREATE  {sql_option}  {viewdb_fullyqview} AS (
            select {src_cols_str} from {presentdb_fullyqtable}
        )
        '''
    else:
        raise ValueError(f'Database not supported {args.dbcon.dbtype}')

    viewdb_fullyqview=f'{args.viewdb}.{args.tgtschema}.{src_table}'
    tgtdb_fullyqview=f'{args.tgtdb}.{args.tgtschema}.{src_table}'

    drop_sql=f'''         DROP TABLE if exists {tgtdb_fullyqview}'''

    try:
        result=args.con.exe(drop_sql)
        args.log.info(f'dropped {tgtdb_fullyqview} {list(result)[0]}')
    except:
        args.log.info(f'NO TABLE TO DROP {tgtdb_fullyqview}')
    sql2=''
    if viewdb_fullyqview.upper() != tgtdb_fullyqview.upper():
        args.con.exe(f"CREATE SCHEMA IF NOT EXISTS  {args.viewdb}.{args.tgtschema}")

        if args.con.dbtype=='snowflake':
            sql2=f'''
            CREATE  {sql_option}  {tgtdb_fullyqview} AS (
                select {src_cols_str} from {viewdb_fullyqview}
            )
            '''
        else:
            raise ValueError(f'Database not supported {args.dbcon.dbtype}')


    return sql1,sql2

#------------------------------  Presentation Layer
def create_present(args,src_table,tgt_view=''):
    args.log.info('AT CREATE PRESENT')
    if not tgt_view: tgt_view=src_table


    src_table_cols=list([col.upper() for col in args.con.get_cols(args.tgtschema,table=src_table,db=args.sourcedb)])
    src_cols_str=','.join(src_table_cols)

    sql_option='or REPLACE TABLE'
    #sql_option='TABLE IF NOT EXISTS'

    src_fullyqtable=f'{args.sourcedb}.{args.tgtschema}.{src_table}'
    tgt_fullyqview=f'{args.presentdb}.{args.tgtschema}.{src_table}'

    args.con.exe(f"create SCHEMA IF NOT EXISTS  {args.presentdb}.{args.tgtschema}")

    if args.con.dbtype=='snowflake':
        sql=f'''
        CREATE  {sql_option}  {tgt_fullyqview} AS (
        with add_id as ( 
            select NULL as BWC_ID,{src_cols_str}
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

def replace_source_table(args,table,replace_table=''):
    if not replace_table: replace_table=table

    args.con.exe(f"create SCHEMA IF NOT EXISTS  {args.sourcedb}.{args.tgtschema}")
    args.con.exe(f"CREATE OR REPLACE TABLE  {args.sourcedb}.{args.tgtschema}.{replace_table}  clone {args.stagingdb}.{args.tgtschema}.{table}")

def append_source_table(args,table,stage_table=''):
    if not stage_table: stage_table=table

    tgt_table_cols=set([col.upper() for col in args.con.get_cols(args.tgtschema,table=table,db=args.sourcedb)])
    stage_table_cols=set([col.upper() for col in args.con.get_cols(args.tgtschema,table=stage_table,db=args.stagingdb)])



    if not tgt_table_cols:
        args.log.info('No columns found, replacing table with new table')
        return 'need to run replace'
    else:
        new_cols_to_add=stage_table_cols.difference(tgt_table_cols)
        removed_cols=set(tgt_table_cols).difference(stage_table_cols)

        fq_src=f"{args.stagingdb}.{args.tgtschema}.{table}"
        fq_tgt=f"{args.sourcedb}.{args.tgtschema}.{stage_table}"

        args.log.info(f'{fq_src} {stage_table_cols}')
        args.log.info(f'{fq_tgt} {tgt_table_cols}')

        args.log.info(f'{fq_src}  {new_cols_to_add} to be added to {fq_tgt}' )
        args.log.info(f'{removed_cols} no longer present in {fq_src} ')

        table_cols_str=','.join(stage_table_cols)
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


def load_data_snowflake(args,src_file,db,schema,table,file_delim,file_term,header):
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
    # fields=get_fields(args,fname)
    # fields_str=','.join(fields)
    #     # Copy from local file

    stagedir=snow_get_stagedir(db,schema,table)

    if snow_exists_stage(args,stagedir):
        result=snow_remove_stage(args,stagedir)
        print('removed',result,'files')

    if snow_exists_stage(args,stagedir):
        args.log.warn(f'Stage not empty')
        raise Warning(f'Stage directory: {stagedir}  not empty')

    snow_put(args,fname,stagedir)

    result = snow_copy_into(args,src_file,db,schema,table,stagedir,file_delim,file_term,header)

    #[{'file': 'DBTEST/x10057301/a/a.csv.gz', 'status': 'LOADED', 'rows_parsed': 3, 'rows_loaded': 3, 'error_limit': 3, 'errors_seen': 0, 'first_error': None, 'first_error_line': None, 'first_error_character': None, 'first_error_column_name': None}]

    tmp_loadfname.write_text(f'{begin} starting\n {time.asctime()} done\n')
    args.log.info(f'marking as done by replacing {tmp_loadfname} {fname} ')

    alter_load_key=f"alter table {db}.{schema}.{table} add column BWC_DW_LOAD_KEY TEXT default '{args.load_key}'";
    args.con.exe(alter_load_key)
    alter_extract_ts=f"alter table {db}.{schema}.{table} add column BWC_DW_EXTRACT_TS TEXT default '{args.load_ts}'";
    args.con.exe(alter_extract_ts)
    alter_row_parse=f"alter table {db}.{schema}.{table} add column BWC_DW_ROW_PARSE INTEGER default {result['rows_parsed']}";
    args.con.exe(alter_row_parse)
    alter_row_load=f"alter table {db}.{schema}.{table} add column BWC_DW_ROW_LOAD INTEGER default {result['rows_loaded']}";
    args.con.exe(alter_row_load)
    alter_error_seen=f"alter table {db}.{schema}.{table} add column BWC_DW_ERROR_SEEN INTEGER default {result['errors_seen']}";
    args.con.exe(alter_error_seen)

    tmp_loadfname.replace(load_fname)

    # cur.execute("COPY table(field1, field2) FROM LOCAL"
    #             " 'data_Jan_*.csv','data_Feb_01.csv' DELIMITER '{delim}'"
    #             " REJECTED DATA 'path/to/write/rejects.txt'"
    #             " EXCEPTIONS 'path/to/write/exceptions.txt'",
    #             buffer_size=65536
    # )


def getcount(args,db,schema,table):
    base_results={}
    count_sql= f'''select count(*) as COUNT from {db}.{schema}.{table}'''
    print('******',list(args.con.fetchdict(count_sql,size=1)))
    tgt_count=list(args.con.fetchdict(count_sql,size=1))[0]['COUNT']
    args.log.info(f'tgt table count ={tgt_count}')
    return tgt_count

def load_table(args,db,schema,src_file,tgt_table,file_delim,file_term,header):
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

    args.log.info,(f'creating tgt_table: {args.stagingdb}.{args.tgtschema}.{tgt_table}')

    if args.tgtdb_dict['type']=='snowflake':
        args.con.exe(f"create SCHEMA IF NOT EXISTS {args.stagingdb}.{args.tgtschema}")
        args.con.exe(f"drop TABLE IF EXISTS {args.stagingdb}.{args.tgtschema}.{tgt_table}")
        ddl_sql=create_ddl(args,db,schema,src_file,tgt_table,file_delim,file_term,header)
        args.con.exe(ddl_sql)
    else:
        raise ValueError(f'unsupported database for loading {args.stagingdb}')


    #now load the data
    args.log.info,(f'loading: {args.stagingdb}.{args.tgtschema}.{tgt_table} into {args.tgtdb}')
    if args.tgtdb_dict['type']=='snowflake':
        load_data_snowflake(args,src_file,args.stagingdb,args.tgtschema,tgt_table,file_delim,file_term,header)
        tgt_count=getcount(args,args.stagingdb,args.tgtschema,tgt_table)
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


def save_results(args,base_results):
    base_results_list=[base_results]
    now=datetime.datetime.now()
    ymd_hms=now.strftime('%Y_%m_%dT%H%M%S') 
    report=f'{args.logdir}/report_{ymd_hms}_{Path(sys.argv[0]).name}.csv'
    inf.write_csv(report,base_results_list,log=args.log,delim=',')


def archive_file(args,aload,archive):
    src=aload['srcfile']
    tgt=Path(args.tgtdata)/src.name
    if aload['check']=='skip':
        if not os.path.exists(src):
            args.log.info(f'no file to archive {tgt}')
            return 0

    if archive=='move':
        args.log.info(f'''MOVE Archive {aload['srcfile']}  to {tgt}''')
        shutil.copyfile(src,tgt)
        src.unlink(src)
    else:
        args.log.info(f'''COPY Archive {aload['srcfile']}  to {tgt}''')
        shutil.copyfile(src,tgt)

def load_file(args,aload):

    file_obj=aload['srcfile']

    if not file_obj.exists():
        if aload['check']=='skip':
            args.log.info(f'''file not found, skipping check = {aload['check']}''')
            return 0
        else:
            raise ValueError(f'{file_obj} not found!')

    args.log.info(f'{aload}')
    fsize=file_obj.stat().st_size
    args.log.info(f"found {aload['srcfile']} {fsize} bytes")
    if fsize==0:
        args.log.info(f"src file empty {aload['srcfile']}")
        return 'empty file'

    tgt_table=aload['tgttable'].upper()
    if not tgt_table:   
        tgt_table=aload['srcfile'].stem.split('.',1)[0].upper()

    file_term=aload['term']
    file_delim=aload['delim']
    if not file_delim: file_delim=args.delim 
    if file_delim==r'\t': file_delim='\t'


    #connect and process

    #stage layer
    load_results=load_table(args,args.stagingdb,args.tgtschema,aload['srcfile'],tgt_table,file_delim,file_term,aload['header'])

    #source layer
    append_result=''

    if aload['action']=='append':
        append_result=append_source_table(args,tgt_table)

    #switch from append to replace for the first time
    if append_result=='need to run replace':
        args.log.info('table needs to be replaced for first run')
        replace_source_table(args,tgt_table)

    if aload['action']=='replace':
        replace_source_table(args,tgt_table) #will run on first use of append

    #present layer
    sql=create_present(args,tgt_table)
    args.con.exe(sql)

    #view layer
    sql1,sql2=create_views(args,tgt_table)
    args.con.exe(sql1)
    args.con.exe(sql2)
    # #logging
    # save_results(args,load_results)
    # args.log.info('-==# Processing is Complete #==-') 

def run_all_load_jobs(args,loads):
    '''
    {'srcfile': WindowsPath('//mswg9/groups/IT/ETL/TRANSFER/PD/RISK_CONTROL/Extract/isiActivities.txt'), 'tgttable': '', 'delim': '[^-^]', 
    'term': '[|-|]', 'action': 'replace', 'archive': True, 'check': True, 'header': False}
    '''
    line=0
    errors={}
    successes=[]
    for aload in loads:
        if not aload:
            line+=1
            args.log.info(f'at next load file line ')
            continue

        fname=aload['srcfile'].name
        try:
            load_file(args,aload)
            archive_file(args,aload,aload['archive'])
            successes.append(fname)
        except:
            err=inf.geterr()
            errors[fname]=err
            args.log.info(f"{fname} {err}")
    args.log.info(f"SUCCESS FILES: {len(successes)} {successes}")
    errmsg=f'ERROR FILES: {len(errors.keys())}  {errors.keys()}'
    args.log.info(errmsg)
    if errors:
        raise ValueError(errmsg)

def build_all_load_jobs(args):
    '''
    {
        'srcfile': '/RISK_CONTROL/Extract/isiFormFieldOptions.txt,isiLocations.txt,isiProvince.txt', 
        'tgttable': '', 'delim': '', 'term': '', 'action': '', 'archive': None},
    '''
    loads=[]

    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        args.log.info(source_dict)
        src=source_dict['srcfile']
        if src.startswith('#'): continue


        print(source_dict)
        if not source_dict['delim']:
            source_dict['delim']=args.delim
        if not source_dict['term']:
            source_dict['term']=args.term


        if source_dict['archive'] not in ['move','copy']:
            raise ValueError('no archive option specified!')

        if source_dict['check'] not in ['skip','error']:
            raise ValueError('no check option specified!')

        if source_dict['header'] not in ['y','n','skip']:
            raise ValueError('no header option specified!')

        if source_dict['action'] not in ['replace','append']:
            raise ValueError('no action option specified!')


        #if '//mswg9/groups/'  not in src:
        #    src=args.dwpath+src
        
        adir_str,afiles_str=src.rsplit('/',1)
        adir=Path(adir_str)

        if '*' in src:
            args.log.info(f'using glob on {adir} with {afiles_str}')
            afiles=list(Path(adir).glob(afiles_str))
            if not len(afiles):
                if source_dict['check']=='skip':
                    args.log.info(f'''file not found, skipping check = {source_dict['check']}''')
                    continue
                else:
                    raise ValueError(f'No files found in {adir}!')
            
        elif not afiles_str:
            args.log.info(f'using iterdir on {adir}')
            afiles=adir.iterdir()
        else:
            #this will handle single as well as multiple files
            args.log.info(f'splitting {afiles_str}')
            afiles = [ Path(adir_str)/afile for afile in afiles_str.split(',') ]

        for afile in afiles:
            new_source_dict=source_dict.copy()
            new_source_dict['srcfile']=afile
            loads.append(new_source_dict)

        loads.append('')
        
    args.log.info(f'processing {len(loads)}')

    return loads
# ---- STANDARD setup Functions

def load_metadata(args):
    header_dict={}
    dtypes_dict={}

    if args.header_file.exists():
        header_data=list(inf.read_csv(args.header_file,delim='\t'))
        for row in header_data:
            header_dict[row['table']]=row['header']

    if args.dtypes_file.exists():
        dtypes_data=list(inf.read_csv(args.dtypes_file,delim='\t'))
        for row in dtypes_data:
            if row['table'] not in dtypes_dict:
                dtypes_dict[row['table']]={}

            dtypes_dict[row['table']][row['column']]=row['dtype']

    return header_dict,dtypes_dict

def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    codedir=str(Path(f"{os.environ['USERPROFILE']}")/'bwcroot/bwcenv')
 
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
    #req
    parser.add_argument( '--load_version', required=True,help='src file to load')
    parser.add_argument( '--tgtdir', required=True,help='data target /env/db/schema')
    #boolean
    parser.add_argument( '--force', default=False,action='store_true',help='force view replacement')
    #parser.add_argument( '--header', default=False,action='store_true',help='default no file header present')
    #parser.add_argument( '--archive', default=True,action='store_false',help='if true move file')
    #optional
    #--choices
    #parser.add_argument( '--action',  choices=['replace','append'],default='replace',help='replace or append table in source layer')
    #parser.add_argument( '--check',  choices=['skip','error'],default='skip',help='replace or append table in source layer')
    #parser.add_argument( '--archive',  choices=['move','copy'],default='move',help='replace or append table in source layer')

    #--standard
    #parser.add_argument( '--dwpath', default='//mswg9/groups/IT/ETL/TRANSFER/PD',help='bwcstd or custom header')
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

    #--- load_key
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    args.load_key=ymd
    args.load_ts=ymd_hms

    #--- default paths and load file
    tgtdir=args.tgtdir.upper()

    inf.build_args_paths(args,use_load_key=True, find_src_load_key=False)
    args.logdir=args.tgtlog

    args.root=Path(__file__).resolve().parent.parent

    args.loaddir=args.root/'bwcfloads'
    if args.load_version:
        args.load_file=args.loaddir/args.tgtenv/args.tgtdb/f'{args.tgtschema}_{args.load_version}.csv'
        args.header_file=args.loaddir/args.tgtenv/args.tgtdb/f'{args.tgtschema}_header.txt'
        args.dtypes_file=args.loaddir/args.tgtenv/args.tgtdb/f'{args.tgtschema}_dtype.txt'

    # -------   setup standard STAGING->SOURCE->VIEWS framework

    #

    #source_fields= args.staging.replace('_STAGING','_SOURCE').split('/')[-2:]
    #args.source='/'+'/'.join(source_fields)

    args.stagingdb=f'{args.tgtenv}_STAGING'
    args.sourcedb=f'{args.tgtenv}_SOURCE'
    args.presentdb=f'{args.tgtenv}_PRESENT'
    args.viewdb=f'{args.tgtenv}_VIEWS'

    dest_final=f'{args.stagingdb} {args.sourcedb} {args.presentdb} {args.viewdb} {args.tgtdir}'

    args.logdir=args.tgtlog

    # -------  other dirs and logging

    args.root=Path(__file__).resolve().parent.parent

    #target dirs
    args.loaddir=args.tgtdata/'loads'
    args.tgt_ddldir=args.tgtdata/'ddl'
    args.tgt_ddldir.mkdir(parents=True, exist_ok=True)

    args.logdir.mkdir(parents=True, exist_ok=True)
    args.loaddir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app='parent',cleanlog=args.cleanlog)
    args.log.debug('Running on:'+os.environ['COMPUTERNAME'])
    args.log.info(f'{dest_final}')
    args.log.info(f'processing from:{args.tgtdir}')
    return args


def main():
    try:
        args=None
        begin=datetime.datetime.now()
        args=process_args()

        args.log.info(f'using:{args.load_version}')

        args.header_dict,args.dtypes_dict=load_metadata(args)
        loads=build_all_load_jobs(args)
        

        #get conn info
        args.tgtdb_dict = dbsetup.Envs[args.tgtenv][args.tgtkey]
        args.tgtdb_dict['db'] = args.tgtdb
        args.log.info(f'using database info for:{args.tgtenv} {args.tgtdb}')
        with  dblib.DB(args.tgtdb_dict,log=args.log,port=args.tgtdb_dict.get('port','')) as args.con:
            run_all_load_jobs(args,loads)

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
Vertica loading

# RUN
    The default is replace, set action to append in order to append

## DEV
python run_fileload_many.py --load_version daily --tgtdir /uat/snow_etl/DBTEST/CANARY --eldir c:/temp
python run_fileload_many.py --load_version daily --tgtdir /uat/snow_etl/rub1/DW_EXTERNAL_RISK_CONTROL --eldir c:/temp
python run_fileload_many.py --load_version daily --tgtdir /uat/snow_etl/rub1/DW_EXTERNAL_RISK_CONTROL --eldir c:/temp --action append

## UAT
python run_fileload_many.py --load_version daily --tgtdir /uat/snow_etl/rub1/DW_EXTERNAL_RISK_CONTROL 

# Setup
bwcenv\bwcfloads
    \uat\RUB1\DW_EXTERNAL_RISK_CONTROL
        daily.csv

# What happens
python run_fileload_many.py --load_version daily --tgtdir /uat/snow_etl/UAT_VIEWS/DW_EXTERNAL_RISK_CONTROL
    STAGING.*->SOURCE.*->PRESENT.*->VIEWS.DW_EXTERNAL_RISK_CONTROL-> RUB1.DW_EXTERNAL_RISK_CONTROL

python run_fileload_many.py --load_version daily --tgtdir /uat/snow_etl/rub1/DW_EXTERNAL_RISK_CONTROL
    STAGING.*->SOURCE.*->PRESENT.*->VIEWS.DW_EXTERNAL_RISK_CONTROL-> RUB1.DW_EXTERNAL_RISK_CONTROL

#CONFIG FILE

```
srcfile~tgttable~delim~term~action~archive
 
#---- defaults
#srcfile: /IT/ETL/TRANSFER/PD used unless //mswg9/groups is specified
#tgttable:  srcfile
#delim: \t
#term: \n
#action: replace, append
#archive: y
 
#---- transfers
#Risk control
/RISK_CONTROL/Extract/~~~~
/RISK_CONTROL/Extract/*.txt~~~~
/RISK_CONTROL/Extract/isiAccountExt.txt~~~~
/RISK_CONTROL/Extract/isiAccountExt.txt,isiActivities.txt,isiCompanies.txt,isiContacts.txt,isiExtFields.txt~~~~
```

#line 1  full directory
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\AccountAttachments.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\AccountAttachments.txt.final.gz
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\ArchivedAttachments.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\ArchivedAttachments.txt.final.gz

#line 2 glob directory
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\AccountAttachments.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\ArchivedAttachments.txt

#line 3 one file
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\isiAccountExt.txt

#line 4 many files
\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\isiAccountExt.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\isiActivities.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\isiCompanies.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\isiContacts.txt
\\mswg9\groups\IT\ETL\TRANSFER\PD\RISK_CONTROL\Extract\isiExtFields.txt

'''
