import types,os,json,argparse,datetime,sys,warnings
from pathlib import Path

warnings.filterwarnings("ignore")

#other libs
import requests, pandas as pd

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

#bwc libs
from bwcenv.bwclib import inf
from bwcsetup import restsetup

# ----------------------- infrastructure
def json2csv(file_out,thejson):
    '''
    expects rows of dicts
    data = [
    {"id": 1, "name": {"first": "Coleen", "last": "Volk"}},
    {"name": {"given": "Mark", "family": "Regner"}},
    {"id": 2, "name": "Faye Raker"},
    ]
    '''

    tmpfname=Path(str(file_out)+'.tmp')

    tmpfname.unlink(missing_ok=True)
    df=pd.json_normalize(thejson,sep='_')
    df.to_csv(tmpfname, index=False, mode='w', header=True,sep='\t')
    tmpfname.replace(file_out)

    return len(thejson)


def run_get_query(url,headers,api='',params={},delim='\t'):
    '''
    examples
    https://api.surveymonkey.com/v3/surveys {'per_page': '100'} {'Authorization': 'Bearer xx', 'Accept': 'application/json'}
    '''
    client = requests.session()
    url=f'''{url}{api}'''
    
    #print('----------\n\n',url, params,headers);input('go')
    response = client.get(url, params=params,headers=headers,verify=False)

    if response.status_code != 200:
        raise ValueError(f'Got invalid response {response.status_code} {response.text}')

    json_str=json.dumps(response.json()).replace(delim,' ').replace('\n',' ')
    json_results=json.loads(json_str)

    return json_results

# run_post_query(args.params['url'],args.params['headers'],query_url,post,args.params['username'],args.params['password'])
def run_post_query(url,headers,query_url,post_params,username,password,delim='\t'):
    client = requests.session()
    url=f'''{url}{query_url}'''

    auth=None
    if username:
        auth= requests.auth.HTTPBasicAuth(username, password)

    if auth:
        response = client.post(url, headers=headers,auth =auth,verify=False,json =post_params)
    else:
        response = client.post(url, headers=headers,verify=False,json =post_params)

    if response.status_code != 200:
        raise ValueError(f'Got invalid response {response.status_code} {response.text}')

    json_str=json.dumps(response.json()).replace(delim,'')
    json_results=json.loads(json_str)


    return json_results


def connect_basic(url,headers,username,password):

    response = requests.get(url,headers=headers,query_url='', params= params,auth = requests.auth.HTTPBasicAuth(username, password),verify=False)
    print(response.text)


# -------------------------  Business Logic
def oia(args):
    query_url='/analyticsReportResults'
    tables=[ 'Tableau_Org', 'Tableau_Application', 'Tableau_CaseStudy', 'Tableau_Invoice', 'Tableau_Quote', 'Tableau_LineItem' ]
    queries=[ {'table':table,'query':{"lookupName": table}} for table in tables]

    for query in queries:
        table=query['table'];post=query['query']
        table=inf.clean_name(table)
        jsonfile=args.csvdir/(table+'.json')
        csvfile=args.csvdir/(table+'.csv')
        #csvfile=args.csvdir/(args.srcschema+'.csv')
        result_json=run_post_query(args.params['url'],args.params['headers'],query_url,post,args.params['username'],args.params['password'])
        jsonfile.write_text(json.dumps(result_json))
        cols= inf.clean_cols(result_json["columnNames"])        
        rows=[]
        for row in result_json["rows"]:
            new_row=dict(zip(cols,row))
            rows.append(new_row)

        count=json2csv(csvfile,rows)
        args.log.info(f'wrote  {csvfile} {count} rows')


def surveymonkey(args):
    params={}
    # get all surveys max: 1000
    # https://api.surveymonkey.com/v3/surveys
    url=args.params['url'] 

    all_tables_json=run_get_query(url,args.params['headers'],api=args.srcschema,params=params)
    json_schemafile=args.csvdir/(args.srcschema+'.json')
    json_schemafile.write_text(json.dumps(all_tables_json))
    args.log.info(f'wrote:{json_schemafile}')


    #returns urls to use: like https://api.surveymonkey.com/v3/surveys/303452492
    for table_row in all_tables_json['data']:
        try:
            params={}
            table_id,table_name,table_url=table_row['id'],table_row['title'],table_row['href']

            args.log.info(f'------attempting to extract:{table_url}')

            table_name=inf.clean_name(table_name)

            #----- survey details url
            #https://api.surveymonkey.com/v3/surveys/303452492/details
            table_json=run_get_query(table_url,args.params['headers'],f'/details',params=params)

            jsonfile=args.csvdir/(table_name+'_details.json')
            csvfile=args.csvdir/(table_name+'_detail.csv')

            jsonfile.write_text(json.dumps(table_json))
            args.log.info(f'wrote:{jsonfile}')
            count=json2csv(csvfile,table_json)
            args.log.info(f'wrote:{csvfile}  {count} rows')                

            #---- responses url (join later to details)
            # https://api.surveymonkey.com/v3/surveys/303452492/responses/bulk

            #1st batch
            params={'per_page':'100'}
            table_json=run_get_query(table_url,args.params['headers'],f'/responses/bulk',params=params)
            total=int(table_json['total'])

            all_data=table_json['data']

            inc=0;page=1;alljson = []
            while len(all_data)<total:
                if inc>100: break
                page+=1
                params={'per_page':'100','page':page}
                table_json=run_get_query(table_url,args.params['headers'],f'/responses/bulk',params=params)
                all_data+=table_json['data']
                alljson.append(table_json)

            jsonfile=args.csvdir/(table_name+'_responses.json')
            csvfile=args.csvdir/(table_name+'_responses.csv')

            jsonfile.write_text(json.dumps(alljson))
            args.log.info(f'wrote:{jsonfile}')
            count=json2csv(csvfile,all_data)
            args.log.info(f'wrote:{csvfile}  {count} rows')
        except:
            args.log.info(f'got error {inf.geterr()}')


def process_args():
    os.environ['HTTPS_PROXY']='http://europa:84'
    os.environ['HTTP_PROXY']='http://europa:84'
    #eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"H:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --srcdir /prd/surveymonkey",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='/env/api')
    parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
    #boolean
    #
    #optional
    parser.add_argument( '--load_key', default='',help='load_key to use')
    parser.add_argument( '--delim', default='\t',help='delim to use, e.g. ~ | ')   
    args = parser.parse_args()


    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    args.load_key=ymd
    args.load_ts=ymd_hms
    args.root=Path(__file__).resolve().parent.parent

    #makes args.srcdir,srcenv,srcdb,srckey,srcschema,srcdata,srclog
    inf.build_args_paths(args,use_load_key=True, find_src_load_key=False)

    #-- data setup
    args.csvdir=args.srcdata/'extracts'
    args.csvdir.mkdir(parents=True, exist_ok=True)

    #-- logging setup
    args.logdir=args.srclog
    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'{sys.argv[0]} launched with arguments: {sys.argv[1:]}')

    args.log.info(f'Saving files to: {args.csvdir}')

    return args


def main():

    args=process_args()
    args.params=restsetup.Envs[args.srcenv][args.srckey]   

    args.log.info(f'extracting {args.srcdb} {args.srcschema}')

    if args.srcdb=='surveymonkey':
        surveymonkey( args )
    elif args.srcdb=='oia':
        oia( args )
    else:
        raise ValueError(f'invalid srcdb {args.srcdb}')


main()

'''
python run_extract_rest.py --srcdir /uat/surveyauth/surveymonkey/surveys
python run_extract_rest.py --srcdir /uat/oiaauth/oia/analyticsReportResults
python bwcroot/bwcenv/bwcrun/run_extract_rest.py --srcdir /prd/surveyauth/surveymonkey/surveys
python bwcroot/bwcenv/bwcrun/run_extract_rest.py --srcdir /prd/oiaauth/oia/analyticsReportResults

python bwcroot/bwcenv/bwcrun/run_extract_rest.py --srcdir /prd/surveyauth/surveymonkey/surveys
python bwcroot/bwcenv/bwcrun/run_extract_rest.py --srcdir /prd/oiaauth/oia/analyticsReportResults
'''