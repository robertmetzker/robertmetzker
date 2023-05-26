#std libs 
import sys,argparse,os,datetime,gzip
from pathlib import Path

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

#####beginning of code taken from run_snowload.py 
def create_table(args,dbname,schema,table):
    columns_str='''
        AGRE_ID NUMBER(31,0),
        BILATERAL_IND TEXT,
        CLM_NO TEXT,
        CRNT_IND TEXT,
        DW_CREATE_DTTM TIMESTAMP,
        DW_UPDATE_DTTM TIMESTAMP,
        HIST_EFF_DTM TIMESTAMP,
        HIST_END_DTM TIMESTAMP,
        ICDC_MPD_CODE TEXT
    '''
    sql=f"""CREATE OR REPLACE  TABLE {dbname}.{schema}.{table} ({columns_str} );\n"""
    print(sql)
    result=list(args.tgtcon.exe(sql))[0]
    if 'successfully created' not in result['status']:
        raise Warning(f' did not create table {result}')

######end of code from run_snowload.py

def run_sql(con1,con2,clm_no='',count=0):
    '''
    The 2 streams that are being merged, have to be sorted correctly for the merge to work.
    clm_no had issue in the past, which is why there is trimming. there was an issue with aliasing, which is why there is 1,2,3,4 may be fixed now
    sql2: btrim is used because alias didn't work,trim(icdc_mpd_code) -- code is very complext
    sql1: simpler code so aliasing worked
    We are merging a unique listing of the 3 cols in sql1 with sql2 which has the flag and change dates.
    driving query is sql1: the function returns the batch of rows from sql2 to match sql1
    yield is used in order to prevent high memory usage, and follow etl style coding
    '''
    sql2=''' SELECT trim(clm_no) as clm_no, agre_id, btrim, chg_date, trim(bilat_flag) as bilat_flag from bwc_etl.temp_dw_bilat4_n_y_d_match  order by clm_no, agre_id, btrim, chg_date'''
    if clm_no:
        sql2=f'''SELECT clm_no, agre_id, btrim, chg_date, bilat_flag from bwc_etl.temp_dw_bilat4_n_y_d_match where clm_no= '{clm_no}' order by clm_no,agre_id,btrim,chg_date'''

    query2=con2.fetchrow(sql2)

    sql1='''select trim(clm_no) as clm_no, agre_id, trim(icdc_mpd_code) as icdc_mpd_code  from bwc_etl.temp_dw_bilat1  order by clm_no, agre_id, icdc_mpd_code '''
    if clm_no:
        sql1=f'''select clm_no, agre_id, icdc_mpd_code  from bwc_etl.temp_dw_bilat1  where clm_no= '{clm_no}' order by clm_no, agre_id, icdc_mpd_code '''

    newrows=[]
    row_batch=[]
    rownum=0

    allrow1=con1.fetchrow(sql1)
    for row1 in allrow1:
        claim_no,agre_id,icd =row1.values()
        icd=icd.strip()
        claim_no=claim_no.strip()
        rownum+=1

        for row2 in query2:
            if (claim_no,agre_id,icd) == (row2['CLM_NO'],row2['AGRE_ID'],row2['BTRIM']):
                newrows.append(row2)
            else:
                if newrows:
                    yield newrows
                newrows=[]
                newrows.append(row2)
                break

    if newrows: 
        yield newrows

#claim,agr_id,mpd_code,unique_combo,chg_date,sql_flag,prev_sql_flag,bgn_date,end_date):
#[('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(2005, 10, 3, 0, 0), 'D'), ('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(2005, 10, 4, 0, 0), 'Y'), ('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(2005, 11, 14, 0, 0), 'Y'), ('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(2005, 11, 15, 0, 0), 'Y'), ('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(2006, 5, 15, 0, 0), 'Y'), ('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(2006, 5, 16, 0, 0), 'D'), ('03-890135', 8572367.0, '153.9', '03-8901358572367153.9', datetime.datetime(9999, 12, 31, 0, 0), 'D')]

def transformation(now,rows):
    '''
    This transforms the batch of:clm_no,agre_id,icdc_mpd_code
    The point of the transformation is to collapse rows together of the same bilateral flag into a row w\a date range
    D represents a fake row, used as a marker, if there are no rows in a batch when D arrives, skip it
    '''

    max_date=datetime.datetime(9999, 12, 31, 0, 0)

    prev_date=None

    new_rows=[]

    #current row flag (cibh just referred to legacy naming)
    crnt_cibh_flag='n'
    for count,row in enumerate(rows):
#       ('01-409735', 6243401.0, '847.2', datetime.datetime(2001, 7, 23, 0, 0), 'D')       
        claim,agr_id,mpd_code,chg_date,bi_flag=row.values()

        if chg_date==max_date:
            crnt_cibh_flag='y'
        else:
            crnt_cibh_flag='n'

        #!!!row dropped here
        if bi_flag =='D' :
            if len(new_rows)==0:continue
        
        #for first pass for this batch, initialize the row
        if len(new_rows)==0:
            adict={'CLM_NO':claim,'AGRE_ID':agr_id,'ICDC_MPD_CODE':mpd_code,'HIST_EFF_DTM':chg_date,'HIST_END_DTM':chg_date,'BILATERAL_IND':bi_flag,'CRNT_IND':crnt_cibh_flag,'DW_CREATE_DTTM':now,'DW_UPDATE_DTTM':now}
            new_rows.append(adict)
            continue

        last_row=new_rows[-1]

        #!!!!rows are dropped here, if the flag stays the same
        #whenever the flag changes, make a new row
        #otherwise drop the row and push the date out
        if last_row['BILATERAL_IND'] != bi_flag:
            adict={'CLM_NO':claim,'AGRE_ID':agr_id,'ICDC_MPD_CODE':mpd_code,'HIST_EFF_DTM':chg_date,'HIST_END_DTM':chg_date,'BILATERAL_IND':bi_flag,'CRNT_IND':crnt_cibh_flag,'DW_CREATE_DTTM':now,'DW_UPDATE_DTTM':now}
            new_rows.append(adict)
        else:
             last_row['HIST_END_DTM']=chg_date
#             last_row['crnt_cibh_flag']=crnt_cibh_flag
             if last_row['HIST_END_DTM']==max_date:
                 last_row['CRNT_IND']='y'
             else:
                 last_row['CRNT_IND']='n'                 
        
#    if last_row['end'] == max_date:
#       last_row['end'] = ''

    #filter out all Ds
    #final_rows = [ row for row in new_rows if row['flag']!='D']
    '''
    CLM_NO varchar(30),
    AGRE_ID numeric(31,0),
    ICDC_MPD_CODE varchar(15),
    HIST_EFF_DTM timestamp,
    HIST_END_DTM timestamp,
    CRNT_IND char(1),
    BILATERAL_IND varchar(1),
    DW_CREATE_DTTM timestamp(6),
    DW_UPDATE_DTTM timestamp(6)
    '''

    #final_rows = [ [r['claim'],r['agr_id'],r['mpd_code'],r['start'],r['end'],r['crnt_cibh_flag'],r['flag'],r['create_dttm'],r['update_dttm']] for r in new_rows if r['flag']!='D' and r['start'] !=max_date]
    final_rows = [ r for r in new_rows if r['BILATERAL_IND']!='D' and r['HIST_EFF_DTM'] !=max_date]

    #get rid of 12/31/9999 date, convert to empty string
    final_rows2=[]

    for dict_row in final_rows:
        newrow = {}
        for col,val in dict_row.items():
            if val == max_date:
                val=''
            newrow[col]=val
        final_rows2.append(newrow.copy())
    return final_rows2

# Parse the command line arguments
def process_args():
    #eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --srcdir /dev/cam/base",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='/env/database/schema')
    parser.add_argument( '--tgtdir', required=True,help='/env/database/schema')
    #optional
    parser.add_argument( '--table', default='DW_CLAIM_BILAT_HISTORY',help='table to load into')
    parser.add_argument( '--level', default=1,help='where in call chain this is for logging')
    parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
    parser.add_argument( '--logdir', default='',help='default logging directory, $root/env/conn/schema/load_key/logs')
    parser.add_argument( '--load_key', default='',help='load_key to use')

    args = parser.parse_args()

    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    args.load_key=ymd
    args.load_ts=ymd_hms

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srclog,args,srcenv
    inf.build_args_paths(args, use_load_key=True, find_src_load_key=True)
    args.prefix=''

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir=args.srclog
    args.csvdir=args.srcdata/'extracts/'

 # Directories will be created if they do not already exist.
    args.log=inf.setup_log(args.logdir,app='parent',prefix=args.level*'\t')
    args.log.info(f'{sys.argv[0]} launched with arguments: {sys.argv[1:]}')
    args.csvdir.mkdir(parents=True, exist_ok=True)

    args.begin=''
    args.log.debug(f'args global settings:{args}')
 
    return args

def main():
    '''
    '''
    print('Job started at ' + str(datetime.datetime.now())[0:19]) # format is 'YYYY-MM-DD HH:MM:SS.SSSSSS'

    args = process_args()
    
    # get database connection info based on arguments out of dbsetup
    args.srcdb_dict = dbsetup.Envs[args.srcenv][args.srckey]
    args.srcdb_dict['db'] = args.srcdb
    args.tgtdb_dict = dbsetup.Envs[args.tgtenv][args.tgtkey]
    args.tgtdb_dict['db'] = args.tgtdb
    
    #2 connections are used to merge 2 streams together
    con1=dblib.DB(args.srcdb_dict,log=args.log,port=args.srcdb_dict.get('port',''))
    con2=dblib.DB(args.srcdb_dict,log=args.log,port=args.srcdb_dict.get('port',''))

    args.tgtcon=dblib.DB(args.tgtdb_dict,log=args.log,port=args.tgtdb_dict.get('port',''))

    final_file=args.csvdir/f'{args.table}.csv.gz'

    #print('removing old files',final_file)
    #if os.path.exists(final_file): os.remove(final_file)

    now=datetime.datetime.now()

    #this is for testing only, default is empty
    clm_no=''
    #clm_no='03-835544'
    if True:
        newlist=[]
        for rownum,row_batch in enumerate(run_sql(con1,con2,clm_no=clm_no,count=20)):
            #now calculate bilateral for the batch
            # every group of clm_no,agre_id,icdc_mpd_code is processed in a batch
            #before 
            #('00-300001', 8479707.0, '915.0', datetime.datetime(2000, 1, 13, 0, 0), 'n')
            #('00-300001', 8479707.0, '915.0', datetime.datetime(2000, 1, 29, 0, 0), 'n')
            #('00-300001', 8479707.0, '915.0', datetime.datetime(2000, 1, 30, 0, 0), 'n')
            #('00-300001', 8479707.0, '915.0', datetime.datetime(9999, 12, 31, 0, 0), 'n')]
            #after collapsed into one row, all is of the same type
            #so new date range is from 1/13/2000 to null with only 1 row
            #CLM_NO,AGRE_ID,ICDC_MPD_CODE,HIST_EFF_DTM,HIST_END_DTM,CRNT_IND,BILATERAL_IND,DW_CREATE_DTTM,DW_UPDATE_DTTM 
            #['00-300001', 8479707.0, '915.0', datetime.datetime(2000, 1, 13, 0, 0), '', 'y', 'n', 
            #datetime.datetime(2017, 3, 14, 15, 51, 42, 800213), datetime.datetime(2017, 3, 14, 15, 51, 42, 800213)]]
            rows=transformation(now,row_batch)

            for item in rows:
                if isinstance(item,list):
                    for _idx,d in enumerate(item):
                        newlist.append(d)
                if isinstance(item,dict):
                    newlist.append(item)

        inf.write_csv(final_file,newlist,log=args.log)

    args.log.info('csv creation completed')

##### NOW LOADING EXTRACT TO SNOWFLAKE


    create_table(args,args.tgtdb,args.tgtschema,args.table)

    args.tgtcon.load_file(args.tgtdb,args.tgtschema,args.table,final_file, header=1)
    
    args.log.info('DW_CLAIM_BILAT_HISTORY load completed')

if __name__=='__main__':
    main()

'''
old example:
python run_bilateral.py --operation bilat   --s_environment dev_cognos  --srcdb vertica --s_schema BWC_ETL   --t_environment dev_cognos --tgtdb vertica --t_schema BWC_ETL --load_type PCMP --pool_size 2'
new example:
python run_bilateral.py --srcdir /prd/snow_etl/RPD1/BWC_ETL --tgtdir /prd/snow_etl/RPD1/DW_REPORT
'''