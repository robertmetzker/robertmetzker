#std libs
import os,time,json,io,argparse,datetime,sys,json,warnings,csv,random, concurrent.futures, logging,gzip
from pathlib import Path
warnings.filterwarnings("ignore")

#other libs

import boto3,numpy
from PIL import Image #pip install Pillow
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
    prog_path = Path(os.path.abspath(__file__))
    root = prog_path.parent.parent.parent
    pyversion = f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))

    #AWS
    os.environ['HTTP_PROXY'] = 'europa:84'
    os.environ['HTTPS_PROXY'] = 'europa:84'
    os.environ['AWS_SHARED_CREDENTIALS_FILE']=str(root/'bwcsetup/aws/credentials')
    os.environ['AWS_CONFIG_FILE']=str(root/'bwcsetup/aws/config')

    print('using path',root,pylibpath)



set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup




def geterr(note='',clean=True):
    import traceback
    error_msg=traceback.format_exc()
    if clean: error_msg=error_msg.replace('\n','')
    return f'BWCERROR:###### {note} {error_msg}'


def run_parallel(afunc,args,parallel=None,log=False,timeout=None,batch=1000):



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

def scrub_row(row,delim=',',term='\n'):
    replace_term=replace_delim=' '
    newrow={}
    for col,val in row.items():
        newval=val
        if isinstance(val,str):
            newval=val.encode('ascii', 'ignore').decode().replace(delim,replace_delim).replace(term,replace_term).replace('\\',replace_delim)
            newval=newval.replace('\x00','').replace('"',replace_delim)                     
        newrow[col]=newval
    dict_row=newrow

    return dict_row 

def write_csv(tgtfile, rows, delim = ','):
    #scrub_rows

    with open(tgtfile, 'w', newline = '') as fw:
        for idx,row in enumerate(rows):
            if idx==0: 
                fields = row.keys()
                writer = csv.DictWriter(fw, fieldnames = fields, delimiter = delim)
                writer.writeheader()

            newrow=scrub_row(row,delim=delim)
            writer.writerow(newrow)


def read_csv(fname,delim=',',term='\n',header=None,log=False):
    '''
    If fieldnames is omitted, the values in the first row of file will be used as the fieldnames. 
    Regardless of how the fieldnames are determined, the dictionary preserves their original ordering.
    https://docs.python.org/3/library/csv.html#csv.DictReader
    '''
    with open(fname) as fr:
        for row in csv.DictReader(fr,fieldnames=header,delimiter=delim):
            yield row

#----- UDS

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

    #client=boto3.client('location',verify=False,config=Config(proxies={'http': 'europa:84','https': 'europa:84'}))
    args.textract_client = boto3.client('textract',verify=False,config=Config(proxies={'http': 'europa:84','https': 'europa:84'}))

def main_parallel(allargs):
    args,row_dict=allargs
    connect_aws(args)
    ref=row_dict['DCMNT_RFRNC'] 
    ref_path=build_path(ref)
    claim_files=find_files(ref_path)
    rows=[]
    for idx,afile in enumerate(claim_files):
        udsdir=Path(args.srcdir)/'extract'
        udsdir.mkdir(parents=True,exist_ok=True)

        pngfile=udsdir/f"{row_dict['CLAIM_NO']}_{afile.name}.png"
        txtfile=pngfile.parent/(pngfile.name+'.txt')
        #if txtfile.exists(): continue

        if not pngfile.exists(): 
            try:
                img2png(afile,pngfile)
            except:
                txtfile.write_text(geterr())
        cleaned_text=thetext='bwc:unknown'
        if not txtfile.exists():
            thetext=run_textract(args,pngfile,txtfile)
        else:
            thetext= txtfile.read_text()

        if thetext:
            cleaned_text=thetext.replace('\n','     ').replace(',',' ')
            
        rows.append(txtfile.name+','+cleaned_text)
    return rows

def get_files(args):
    '''
        add sql to find files already loaded
    '''
    srcdb = dbsetup.Envs[args.srcenv][args.srckey]
    srccon = dblib.DB(srcdb, log=args.log, port = srcdb.get('port',''))


    sql='''SELECT CLAIM_NO,DCMNT_RFRNC,DCMNT_TYPE_CODE,DCMNT_TYPE_TEXT,RCVD_DATE,PAGE_QTY  FROM 
                RPD1.DW_REPORT.DW_TDFIMGE where RCVD_DATE::DATE>current_date()-7;'''

    row_gen = srccon.fetchdict(sql)

    rows=list(row_gen)[:5]
    args.log.info(f'processing {len(rows)}')

    all_args=[]
    for idx,row_dict in enumerate(rows):
        all_args.append([args,row_dict])

    target_csv=Path(args.srcdata)/(args.table+'.csv')
    args.log.info(f'extracting to {target_csv}')

    return all_args


def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='command line args', epilog="Example:python extract.py --srcdir /dev/cam/base", add_help = True)
    #required
    parser.add_argument( '--srcdir', required = True, help = '/env/database/schema')
    parser.add_argument( '--sql', required = False, help = 'sql statement to execute')

    #boolean
    # parser.add_argument( '--limit_rows',default=False,action='store_true',help='Set row limit, default=10')

    #optional
    parser.add_argument( '--eldir', default = eldir, help = 'default directory to dump the files')
    parser.add_argument( '--load_key', default = '', help = 'load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--logdir', default = '', help = 'default logging directory, $root/env/conn/schema/load_key/logs')
    parser.add_argument( '--table', default = 'uds',help = 'specific table to extract when not doing full_schema, e.g. DATE_DIM')

    
    args = parser.parse_args()

    args.aws_cred=os.environ['AWS_SHARED_CREDENTIALS_FILE']
    args.aws_config=os.environ['AWS_CONFIG_FILE']

    #-- set the load key if not specified
    now = datetime.datetime.now()
    args.now = now
    ymd = now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms = now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');

    #alternative way to determine load file location
    inf.build_args_paths(args, use_load_key = True, find_src_load_key = False)
    args.prefix=''

    args.root=Path(__file__).resolve().parent.parent

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir=args.srclog

 # Directories will be created if they do not already exist.
    args.log = inf.setup_log(args.logdir, app='parent')


    return args


def main():

    args=process_args()


    try:



        if 0:
            for row in all_args:
                result=main_parallel(row)
                print(result);input('go') 

        if 1:
            rows=[]
            with  open(target_csv,'w') as fw:
                for idx,result in enumerate(run_parallel(main_parallel,args=all_args,parallel=1)):
                    if idx==0: args.log.info(f'{time.asctime()}, {idx}')
                    if not result: continue
                    try:
                        if idx==0:fw.write('fname,text\n'); continue
                        rows+=result
                        if len(rows)>1000:
                            args.log.info(f'{time.asctime()}, {idx}')
                            astr='\n'.join(rows)
                            fw.write(astr)
                            rows=[]
                    except:
                        args.log.info('error',idx,geterr())           

                if rows:
                    astr='\n'.join(rows)
                    fw.write(astr)
                    rows=[]   


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
Example execution:
    python run_example_db.py --srcdir /dev/oracle_etl/DB/Schema --sql "select *  from pcmp.common_change_tracking where rownum <=2"
    python run_example_db.py --srcdir /prd/oracle_etl/pub1/pcmp
'''    
