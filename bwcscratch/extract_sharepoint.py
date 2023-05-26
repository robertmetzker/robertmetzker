#std libs
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
import subprocess,json,html
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

from bwcenv.bwclib import inf,dblib
from bwcsetup import dbsetup

def process_args():
    '''

    '''
    #etldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

 
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='data source /dev/cred/dev_ods/sharepoint')
    parser.add_argument( '--tgtfile', required=True,help=r'I:\IT\ETL\TRANSFER\PD')

    #optional
    parser.add_argument( '--eldir', default=eldir,help='default directory for logging, data files, etc')
    args = parser.parse_args()

    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14AM
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    args.load_ts=ymd_hms

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srclog,args,srcenv
    #use_load_key=False, find_src_load_key=False 
    #alternative way to determine load file location
    inf.build_args_paths(args,use_load_key=False, find_src_load_key=False)
    args.logdir=args.srclog
    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'processing in:{args.eldir}')
    args.prefix=''

    args.root=Path(__file__).resolve().parent.parent
    args.csvdir=args.srcdata/'extracts'
    args.csvdir.mkdir(parents=True, exist_ok=True)
    args.log.debug(f'Saving files to:{args.csvdir} {args.logdir}')

    return args


def convert(astr):
    conversion= {'_x0020_':'_','_x002f_':'_','_x0023_':'NUM',}
    for k,v in conversion.items():
       if k in astr:  astr=astr.replace(k,v)
    return astr


def fix_json(list_json):
    new_list_json=[]

    for row_dict in list_json:
        adict={}
        for k,v in row_dict.items():
            v=v.replace(',',' ').strip()
            newk=convert(k)
            adict[newk]=v
            new_list_json.append(adict)

    return new_list_json


def json2csv(outfile,fixed_list_json):
    unique_list = [dict(t) for t in {tuple(d.items()) for d in fixed_list_json}]
    fields=unique_list[0].keys()
    with open(outfile,'w',newline='') as fw:
        writer=csv.DictWriter(fw,fieldnames=fields)
        writer.writeheader()
        for idx,row in enumerate(unique_list):
            writer.writerow(row)
    return fields


def call_powershell(uri):
    # TODO build powershell command outside and run inside
    # TODO check if result.stderr is useful; at least log it; add stderr
    result = subprocess.run(['powershell', '-Command', f'Invoke-WebRequest -Uri {uri} -UseDefaultCredentials | Select-Object -Expand Content'], capture_output=True)
    return result.stdout


def get_dbcon():
    print('Getting Snowflake DB connection')
    env,db='dev','snow_etl'
    tgtdb = dbsetup.Envs[env][db]
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con=dblib.DB(tgtdb,log='',port=tgtdb.get('port',''))
    con.exe('use database DEV_ODS')
    # con.exe('create stage STAGE_EXAM')
    return con


def snow_put(path):
    '''
        requires the full path
    put file://I:\IT\ETL\nielsenjf\snowflake\extracts_active\ADR_TYP_INFSPLIT_2700000.gz @~/DBTEST/X10057301/ADR_TYP/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';
    '''
    stage_dir='@~/DEV_ODS/SHAREPOINT/STAGE_EXAM/'
    print(f'Loading file into {stage_dir}')
    if '\\' in path:
        fname = path.split('\\')[-1]
    else:
        fname = path.split('/')[-1]
        
    stage_cmd=f'''put file://{path} {stage_dir} auto_compress=true;'''
    print(f'\t{stage_cmd}')
    con=get_dbcon()
    result=con.exe(stage_cmd)

    table='EXAM_SCHEDULE'
    results=snow_copy_into(con,table,stage_dir, fname)

    if results.get('errors_seen',0) >0:
        print('*'*80,' ERROR ','*'*80)
        print(f'{results}\n','*'*167)
    else:
        print(f"\t\t:: LOADED: {results.get('rows_loaded',0) } rows\n\n" )
    return 


def snow_copy_into(con,table,stage_dir, fname):
    file_format=f"""file_format =  (type = csv field_delimiter = ',' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  """
    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""

    copy_cmd=f"""copy into DEV_ODS.SHAREPOINT.{table} from {stage_dir}{fname}.gz {file_format} on_error='continue'; """
    # copy_cmd=f"""copy into DEV_ODS.SURVEY_MONKEY.{table} from {stage_dir} FILES = '{fname}.gz' FILE_FORMAT='DEV_ODS.SURVEY_MONKEY.SURVEY_CSV' on_error='continue'; """
    print(f'\t{copy_cmd}')
    result=list(con.exe(copy_cmd))
    print(f'\n\tCOPY INTO complete...')
    # print(f'\n\tCOPY INTO RESULTS: {str(result)}\n\n')
    return result[0]


def create_table_exam_schedule(con,fields):
    fields_list=[]
    for field in fields:
        field=field.replace('.','_').lstrip('_')
        fields_list.append( f'{field} text' )
    fields_str=', '.join(fields_list)
    create_command=f'create or replace table DEV_ODS.SHAREPOINT.EXAM_SCHEDULE ({fields_str})'
    print(f'Creating EXAM_SCHEDULE table on Snowflake \n{create_command}')
    con.exe(create_command)


def main():

    args=None
    args=process_args()
    print('got',args)
    #logs to E:\EXTRACTS\a73465\EL\dev\cred\dev_ods\sharepoint\logs
    print(f'Logging to {args.srcdir}')
    # args.log.debug('debug message')
    # args.log.info('info message')
    # args.log.warning('warn message')
    # args.log.error('error message')
    # args.log.critical('critical message')
    
    try:

        uri = 'http://bwcspteam/claims/teams/Lists/Block%20Scheduling/Allitemsg.aspx'

        response = call_powershell(uri)

        content = response.decode('ascii','ignore').strip()

        start=content.find('{ "Row" :')+9
        end=content[start:].find(']')
        json_output = content[start:start+end]+']'
        list_json=json.loads(json_output)
        fixed_list_json= fix_json(list_json)

        if '/' in args.tgtfile or '\\' in args.tgtfile:
            outfile=args.tgtfile
        else:
            outfile=args.csvdir/args.tgtfile
        fields=json2csv(outfile,fixed_list_json)
        print(f'Wrote to {outfile}')

        con=get_dbcon()
        print('Connected to Snowflake DB')

        create_table_exam_schedule(con,fields)
        
        snow_put(outfile)
        

        
        # Example web response
        # </description>
        #       <author>Smith James</author>
        #       <pubDate>Mon, 03 Apr 2023 16:08:29 GMT</pubDate>
        #       <guid isPermaLink="true">http://bwcspteam/claims/teams/Lists/Block Scheduling/DispForm.aspx?ID=230872</guid>
        #     </item>
        #     <item>
        #       <title>Internal, Occupational</title>
        #       <link>http://bwcspteam/claims/teams/Lists/Block Scheduling/DispForm.aspx?ID=230930</link>
        #       <description><![CDATA[<div><b>COUNTY/CITY:</b> Cuyahoga </div>
        # <div><b>DOCTOR:</b>  Rd. Ste. 210 44124 Bldg.</div>
        # <div><b>DATE AND TIME:</b> 5/23/2023 2:15 PM</div>
        # <div><b>EXAM TYPE:</b> C92</div>
        # <div><b>CLAIM #:</b> XXXXXX</div>
        # <div><b>IW NAME:</b> John smith</div>
        # <div><b>SCHEDULER:</b> KAG</div>
        # <div><b>COMMENT:</b> C92 Physical</div>
        # ]]></description>

    
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
Notes

    python extract_sharepoint.py --srcdir /dev/cred/dev_ods/sharepoint --tgtfile E:/EXTRACTS/a73465/EL/dev/cred/dev_ods/sharepoint/data/extracts/extract.csv
'''