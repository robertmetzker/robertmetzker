#std libs
import os,time,json,io,argparse,datetime,sys,json,warnings,csv,random, concurrent.futures, logging,gzip
from pathlib import Path
warnings.filterwarnings("ignore")

#other libs
#pip install opencv-python and Pillow
import boto3,numpy
from PIL import Image
from PIL import ImageSequence
from botocore.config import Config
import botocore


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
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

#----- UDS


def run_parallel(afunc,args,parallel=None,log=False,timeout=None,batch=1000):
    '''
    Original ruin_parallel appends all results to a list, if results are large, list could get large
    This version yields. This will allo processing of millions of documents
    '''

    if timeout: timeout=timeout*60
    msg=f'{afunc.__name__} size: {len(args)} '
    if log:
        if isinstance(log,logging.Logger): log.info(msg)
        else: print(msg)

    with concurrent.futures.ProcessPoolExecutor(max_workers=parallel) as executor:

        for i in range(0, len(args), batch):
                tosubmit=args[i:i+batch]
                jobs=[executor.submit(afunc,s) for s in tosubmit ]

                try:
                    for idx,f in enumerate(concurrent.futures.as_completed(jobs,timeout=timeout)):
                        try:
                            yield f.result()
                        except:
                            if isinstance(log,logging.Logger):  logging.exception("!!!###  - FAILURE -  ###!!!")
                            else: print('problem with thread:'+geterr())
                            yield 'bwcerror: '#+geterr()

                except concurrent.futures._base.TimeoutError:
                    if log: log.info('Got timeout shutting down')
                    for pid,aproc in  executor._processes.items():
                        if log: log.info(str(aproc))
                        aproc.terminate()
                        executor.shutdown()
                    if log: log.info('PROCESSES shut down')
                    raise 



def img2png(src,tgt):
    tmpfname=Path(str(tgt)+'.tmp')
    tmpfname.unlink(missing_ok=True)

    # iarray = cv2.imread(str(src))
    # img = Image.fromarray(iarray)

    try:

        with Image.open(src) as im:
            im.save(tmpfname, "PNG")
        tmpfname.rename(tgt)
    except:
        tmpfname.unlink(missing_ok=True)
        raise
    return 'done'

def build_path(ref):
    path1='/0'+str(int(ref.strip()[-7:],base=16)).zfill(9)[:3]
    path2='0'+str(int(ref.strip()[-7:],base=16)).zfill(9)[3:6]
    file_path=Path(path1,path2,ref)
    return str(file_path)
   

def find_files(subpath):
    root_dirs=['IMAGE044','IMAGE043','IMAGE042','IMAGE041','IMAGE040','IMAGE039','IMAGE038','IMAGE037',
        'IMAGE036','IMAGE035','IMAGE034','IMAGE033','IMAGE032','IMAGE031','IMAGE030','IMAGE029',
        'IMAGE028','IMAGE027','IMAGE026','IMAGE025','IMAGE024','IMAGE023','IMAGE022','IMAGE021',
        'IMAGE020','IMAGE019','IMAGE018','IMAGE017','IMAGE016','IMAGE015','IMAGE014','IMAGE013',
        'IMAGE012','IMAGE011','IMAGE010','IMAGE009','IMAGE008','IMAGE007','IMAGE006','IMAGE005',
        'IMAGE004','IMAGE003','IMAGE002','IMAGE001']

    #\\Imagefileprod\DISC  \IMAGE001 \0173\0424\JA563E63
    for adir in root_dirs:
        full_path=Path(r'//Imagefileprod/DISC/')/(adir+'/'+subpath)
        if full_path.exists():
            docs=list(full_path.glob('*.img'))
            return docs

#----- TEXTRACT
def read_file(afile):
    with open(afile, 'rb') as doc:
        img = bytearray(doc.read())
    return img

def get_rows_textract(args,data,page=0):
    rows=[]

    for ablock in data['Blocks']:
        if ablock ["BlockType"]!='LINE': continue
        if page: 
             if ablock["Page"] != page: continue

        confidence=ablock["Confidence"]
        top=round(ablock["Geometry"]["BoundingBox"]["Top"],2)
        text=ablock['Text'].replace('\t','').strip()
        #rows.append([top,confidence,text])
        rows.append(text)
    return '\n'.join(rows)

def run_textract(args,src,tgt,overwrite=False):
    try:
        #print('\t\tat',src,time.asctime())
        if not tgt: 
            tgt=src.parent/(src.name+'.txt')

        tgt_json=tgt.parent/(tgt.stem+'.json')
        tgt_json_gz=tgt.parent/(tgt.stem+'.json.gz')

        if not overwrite:
            if tgt.exists():
                return tgt.read_text()

        tmp=src.parent/(src.name+'.txt.tmp')
        filebytes=read_file(src)

        response=args.textract_client.detect_document_text(Document={'Bytes': filebytes})

        the_text=get_rows_textract(args,response)
        the_text_ascii=the_text.encode("ascii", "ignore").decode()
        tmp.write_text(the_text_ascii)
        tmp.replace(tgt)
        return the_text_ascii
    except botocore.exceptions.ClientError as e:
        #print(e.response)
        if e.response['Error']['Code'] == 'ExpiredTokenException':
            #print(time.asctime()+'Please renew AWS credentials')
            return time.asctime()+'Please renew AWS credentials'
        else:
            pass #print(e)
    except Exception as e:
        pass #print(e)


#--------------- Setup
def connect_aws(args):
    #print('using credentials',args.aws_cred)
    os.environ['AWS_SHARED_CREDENTIALS_FILE']=str(args.aws_cred)
    os.environ['AWS_CONFIG_FILE']=str(args.aws_config)

    #proxy
    os.environ['HTTP_PROXY'] = 'europa:84'
    os.environ['HTTPS_PROXY'] = 'europa:84'
    if boto3.__version__.startswith('1.15'):
        os.environ['HTTP_PROXY'] = 'http://europa:84'
        os.environ['HTTPS_PROXY'] = 'http://europa:84'


    #client=boto3.client('location',verify=False,config=Config(proxies={'http': 'europa:84','https': 'europa:84'}))
    args.textract_client = boto3.client('textract',verify=False,config=Config(proxies={'http': 'europa:84','https': 'europa:84'}))

def main_parallel(allargs):
    args,row_dict=allargs
    connect_aws(args)

    #args.log=inf.setup_log(args.logdir,app=f'child_{table}',prefix=(args.level+1)*'\t')


    ref=row_dict['DCMNT_RFRNC'] 
    row_dict['DOCMNT_TEXT']=''
    ref_path=build_path(ref)
    claim_files=find_files(ref_path)
    rows=[]
    for idx,afile in enumerate(claim_files):

        pngfile=Path(args.csvdir)/f"{row_dict['CLAIM_NO']}_{afile.name}.png"
        txtfile=pngfile.parent/(pngfile.name+'.txt')
        #if txtfile.exists(): continue

        if not pngfile.exists(): 
            try:
                img2png(afile,pngfile)
            except:
                txtfile.write_text(inf.geterr())
        cleaned_text=thetext='bwc:unknown'
        if not txtfile.exists():
            thetext=run_textract(args,pngfile,txtfile)
        else:
            thetext= txtfile.read_text()

        if thetext:
            cleaned_text=thetext.replace('\n','     ').replace(args.delim,' ')
            row_dict['DOCMNT_TEXT']+=cleaned_text

    return row_dict

#---------SINGLE THREADED --------------------------

def create_views(args,con,src_table,tgt_view=''):
    args.log.info('AT CREATE VIEWS')

    #src_table_cols=set([col.upper() for col in args.con.get_cols(presentschema,table=src_table,db=presentdb)])
    src_table_cols = []
    coldict = {}

    for col in con.get_cols(args.tgtschema,table=src_table,db=args.presentdb):
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

    presentdb_fullyqtable=f'{args.presentdb}.{args.tgtschema}.{src_table}'
    viewdb_fullyqview=f'{args.viewdb}.{args.tgtschema}.{src_table}'

    con.exe(f"create SCHEMA IF NOT EXISTS  {args.viewdb}.{args.tgtschema}")

    if con.dbtype=='snowflake':
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
        result=con.exe(drop_sql)
        args.log.info(f'dropped {tgtdb_fullyqview} {list(result)[0]}')
    except:
        args.log.info(f'NO TABLE TO DROP {tgtdb_fullyqview}')
    sql2=''
    if viewdb_fullyqview.upper() != tgtdb_fullyqview.upper():
        con.exe(f"CREATE SCHEMA IF NOT EXISTS  {args.viewdb}.{args.tgtschema}")

        if con.dbtype=='snowflake':
            sql2=f'''
            CREATE  {sql_option}  {tgtdb_fullyqview} AS (
                select {src_cols_str} from {viewdb_fullyqview}
            )
            '''
        else:
            raise ValueError(f'Database not supported {args.dbcon.dbtype}')

    con.exe(sql1)
    con.exe(sql2)

    return sql1,sql2

def create_present(args,con,src_table,tgt_view=''):
    args.log.info('AT CREATE PRESENT')
    if not tgt_view: tgt_view=src_table


    src_table_cols=list([col.upper() for col in con.get_cols(args.tgtschema,table=src_table,db=args.sourcedb)])
    src_cols_str=','.join(src_table_cols)

    sql_option='or REPLACE TABLE'
    #sql_option='TABLE IF NOT EXISTS'

    src_fullyqtable=f'{args.sourcedb}.{args.tgtschema}.{src_table}'
    tgt_fullyqview=f'{args.presentdb}.{args.tgtschema}.{src_table}'

    con.exe(f"create SCHEMA IF NOT EXISTS  {args.presentdb}.{args.tgtschema}")

    sql=f'''
    CREATE  {sql_option}  {tgt_fullyqview} AS (
    with add_id as ( 
        select NULL as BWC_ID,{src_cols_str}
        from {src_fullyqtable}
    )
            select {src_cols_str},BWC_ID from add_id
    )
    '''
    con.exe(sql)

    return sql


def load_stage_sql(args,fname,db,schema,table,cols_str,delim):
    '''
    stage, To remove all files for a specific directory, include a forward-slash (/) at the end of the path
    select 'connected' as connected,current_timestamp(2)::TEXT";
    create or replace table dbtest.x10057301.processed_content (ref TEXT,content TEXT );
    rm @~/dbtest/x10057301/processed_content/;
    PUT 'file:///TWG_APP_RECD_DATE.csv' '@"DBTEST"."10077936".%"TWG_APP_RECD_DATE"/ui1668700955574';
    copy into dbtest.x10057301.processed_content from @~/dbtest/x10057301/processed_content/
                  file_format =  (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  
                  ON_ERROR = 'CONTINUE';
                  --PURGE = TRUE;
    '''
    
    stage=f'@~/{db}/{schema}/{table}/'
    fq_table=f'{db}.{schema}.{table}'


    script=f'''
create schema  IF NOT EXISTS {db}.{schema};
create or replace table {fq_table} ( {cols_str} );
list {stage};
rm {stage};
PUT file://{str(fname)} {stage} auto_compress=true;
copy into {fq_table} from {stage}
                file_format =  (type = csv field_delimiter = '{delim}' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  
                ON_ERROR = 'CONTINUE'
                PURGE = TRUE;
alter table {db}.{schema}.{table} add column BWC_DW_LOAD_KEY TEXT default '{args.load_key}';
alter table {db}.{schema}.{table} add column BWC_DW_EXTRACT_TS TEXT default '{args.load_ts}';
    '''

    return script.strip()


def append_source_table(args,con,table,stage_table=''):
    if not stage_table: stage_table=table

    tgt_table_cols=set([col.upper() for col in con.get_cols(args.tgtschema,table=table,db=args.sourcedb)])
    stage_table_cols=set([col.upper() for col in con.get_cols(args.tgtschema,table=stage_table,db=args.stagingdb)])

    if not tgt_table_cols:
        args.log.info('No columns found, replacing table with new table')

        con.exe(f"create SCHEMA IF NOT EXISTS  {args.sourcedb}.{args.tgtschema}")
        con.exe(f"CREATE OR REPLACE TABLE  {args.sourcedb}.{args.tgtschema}.{table}  clone {args.stagingdb}.{args.tgtschema}.{table}")
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
            con.exe(alter_sql)
        insert_sql =f"INSERT INTO {fq_tgt} ({table_cols_str}) SELECT {table_cols_str} from {fq_src}"
        con.exe(insert_sql)

def load_table(args):

    #--------------- staging

    col_dtypes={'RCVD_DATE':'DATE','PAGE_QTY':'INT',}
    cols=[]

    total_cols=args.srccols+',DOCMNT_TEXT'

    for col in total_cols.split(','):
        col=col.strip()
        col_dtype=col_dtypes.get(col,'TEXT')
        cols.append(f'{col.strip()} {col_dtype}' )
    colstr=','.join(cols)

    with dblib.DB(args.tgtdb_dict,log=args.log,port=args.tgtdb_dict.get('port','')) as con:
        stage_sql=load_stage_sql(args,args.outfile,args.stagingdb,args.tgtschema,args.tgttable,colstr,args.delim)
        for sql in stage_sql.split(';'):
            sql=sql.strip()
            if not sql: continue
            command_result=False
            result=list(con.exe(sql))
            if not result: continue
            result=result[0]
            if 'status' in result:
                status=result['status'].upper()
                if 'SUCCESS' in status or 'SUCCEED' in status or 'LOADED' in status:
                    command_result=True
                if not command_result:
                        raise ValueError(f'Problem loading table {args.stagingdb}.{args.tgtschema}.{args.tgttable} : {result}')
            args.log.info(f'results= {result}')

        append_source_table(args,con,args.tgttable)
        create_present(args,con,args.tgttable)
        create_views(args,con,args.tgttable)

def get_new_docs(args):
    args.srcdb_dict = dbsetup.Envs[args.srcenv][args.srckey]
    args.tgtdb_dict = dbsetup.Envs[args.tgtenv][args.tgtkey]
    src_fq_table=f'{args.srcdb}.{args.srcschema}.{args.srctable}'
    tgt_fq_table=f'{args.tgtdb}.{args.tgtschema}.{args.tgttable}'
    sql=f'''

        with src_rows as (
             SELECT {args.srccols}  FROM {src_fq_table} 
            where RCVD_DATE::DATE>current_date()-14 order by RCVD_DATE,CLAIM_NO,DCMNT_RFRNC limit 10000
        )
        , tgt_rows as (
             SELECT {args.srccols}  FROM {tgt_fq_table}
        )

        SELECT * FROM src_rows
        MINUS 
        SELECT * FROM tgt_rows 
            
            
        '''

    with  dblib.DB(args.tgtdb_dict,log=args.log,port=args.tgtdb_dict.get('port','')) as con:
        rows= list(con.fetchdict(sql,size=500))
        return rows

#--------MAIN ---------------------------

def process_args():
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    prog=os.path.basename(sys.argv[0])
    epilog=f'python {prog} --envdir I:/Data_Lake/AWS_MEDICAL/POC'

    parser = argparse.ArgumentParser(description='command line args',epilog=epilog,add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='/env/database/schema/[table]')
    parser.add_argument( '--tgtdir', required=True,help='data target /env/db/schema')
    #parser.add_argument( '--csv', help='file that contains files to process',required=True)
    #boolean
    parser.add_argument( '--random', default=False,action='store_true',help='for concurrency process in a random order')

    #optional
    #parser.add_argument( '--logdir', default='I:/',help='where to send logfiles')
    parser.add_argument( '--srctable', default='DW_TDFIMGE', help='')
    parser.add_argument( '--srccols',  default='RCVD_DATE,CLAIM_NO,DCMNT_RFRNC,DCMNT_TYPE_CODE,DCMNT_TYPE_TEXT,PAGE_QTY', help='')
    parser.add_argument( '--tgttable', default='uds', help='')
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--delim', default=r'\t',help='delim to use')
    parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
    parser.add_argument( '--level', default=1,help='where in call chain this is for logging')
    parser.add_argument( '--parallel', default=50,type=int,help='num of extracts to do in parallel, default= 1 single threaded mode ')
    args = parser.parse_args()

    args.root=Path(__file__).resolve().parent.parent
    args.bwcsetup=Path(__file__).resolve().parent.parent.parent/'bwcsetup'
    #-- setup environment

    #aws
    args.aws_cred=args.bwcsetup/'aws/credentials'
    args.aws_config=args.bwcsetup/'aws/config'

    args.prog=Path(sys.argv[0]).name

    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14AM
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key=ymd
    args.load_ts=ymd_hms

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srclog,args,srcenv
    #use_load_key=False, find_src_load_key=False 
    #alternative way to determine load file location
    inf.build_args_paths(args,use_load_key=True, find_src_load_key=True)
    args.csvdir=args.tgtdata/'extracts'
    args.csvdir.mkdir(parents=True, exist_ok=True)
    args.outfile=args.csvdir/(args.tgttable+'.csv')

    args.stagingdb=f'{args.tgtenv}_STAGING'
    args.sourcedb=f'{args.tgtenv}_SOURCE'
    args.presentdb=f'{args.tgtenv}_PRESENT'
    args.viewdb=f'{args.tgtenv}_VIEWS'
    dest_final=f'{args.stagingdb} {args.sourcedb} {args.presentdb} {args.viewdb} {args.tgtdir}'


    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent',prefix=args.level*'\t')
    args.log.info(f'{sys.argv[0]} launched with arguments: {sys.argv[1:]}')
    args.log.debug(f'Saving docs to:{ args.outfile} {args.logdir}')
    args.log.info('Running on:'+os.environ['COMPUTERNAME'])
    args.log.info(f'{dest_final}')
    args.log.info(f'processing from:{args.tgtdir}')
 

    return args

def get_docs(args,all_args):
    if args.parallel==1:
        for row in all_args:
            result=main_parallel(row)
            print(result);break
    else:
        args.log.info(f'writing to: {args.outfile}')

        for idx,result in enumerate(run_parallel(main_parallel,args=all_args,parallel=args.parallel)):
            if not random.randint(0,200): args.log.info(f'at {idx}')
            yield(result)
        

def main():

    args=process_args()


    all_args=[]
    for idx,row_dict in enumerate(get_new_docs(args)):
        all_args.append([args,row_dict])

    args.log.info(f'Need to extract: {len(all_args)} Documents')

    file_delim=args.delim
    if file_delim==r'\t': file_delim='\t'
    inf.write_csv(args.outfile,get_docs(args,all_args),log=args.log,delim=file_delim,sortit=False)
    load_table(args)

    args.log.info(f'Done')
        

if __name__=='__main__':
    main()

r'''
python run_doc_load.py  --srcdir /prd/snow_etl/rpd1/dW_REPORT  --tgtdir /dev/snow_etl/DEV_VIEWS/DOCS --parallel 2

'''
