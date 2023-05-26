#std libs
import sys,argparse,os,gzip,datetime,shutil
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

def get_policy_data(p_policy, p_pol_data_dict):
    '''
    get data for specified policy
    '''

    pol_data_list = p_pol_data_dict.get(p_policy)

    if pol_data_list:
        agre_id = pol_data_list[0]
        cust_id = pol_data_list[1]
    else:
        agre_id = 0
        cust_id = 0

    return agre_id, cust_id


def create_record(args,p_pred_policy, p_succ_policy, p_pol_data_dict, p_outfile, p_errfile):
    '''
    create a record for specified predecessor/successor policies
    '''

    err_rec = None
    err_msg = 'Data not found for '

    pred_bus_seq = succ_bus_seq = 0
    create_dttm = update_dttm = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

    pred_agre_id, pred_cust_id = get_policy_data(p_pred_policy, p_pol_data_dict)

    if p_succ_policy == p_pred_policy:
        # if predecessor and successor policies are the same then no need to call get_policy_data
        succ_agre_id = pred_agre_id
        succ_cust_id = pred_cust_id
    else:
        succ_agre_id, succ_cust_id = get_policy_data(p_succ_policy, p_pol_data_dict)

    #pred_agre_id and cust_id can never be zero
    if pred_agre_id == 0 and pred_cust_id == 0 and succ_agre_id == 0 and succ_cust_id == 0:
        err_rec = err_msg + 'predecessor policy ' + p_pred_policy + ' and successor policy ' + p_succ_policy
    elif pred_agre_id == 0 and pred_cust_id == 0:
        err_rec = err_msg + 'predecessor policy ' + p_pred_policy + ' (successor policy = ' + p_succ_policy + ')'
    elif succ_agre_id == 0 and succ_cust_id == 0:
        err_rec = err_msg + 'successor policy ' + p_succ_policy + ' (predecessor policy = ' + p_pred_policy + ')'

    if err_rec:
        p_errfile.write(err_rec + args.newline)
    else:
        record = ([str(pred_agre_id),str(pred_cust_id),p_pred_policy,str(pred_bus_seq),str(succ_agre_id),str(succ_cust_id),p_succ_policy,str(succ_bus_seq),create_dttm,update_dttm])
        out_rec = ','.join(record)
        p_outfile.write(out_rec + args.newline)

    return err_rec

def get_final_successor(p_pred_policy, p_pol_comb_dict):
    '''
    get the final successor policy for specified predecessor policy
    '''

    # populate variables with initial values
    temp_pred_policy = p_pred_policy
    temp_succ_policy = p_pred_policy

    # go through the combination chain until no more successor policy is found
    while temp_succ_policy:
        temp_succ_policy = p_pol_comb_dict.get(temp_pred_policy)

        if temp_succ_policy:
            # if successor policy is found, use it as predecessor policy in next lookup
            temp_pred_policy = temp_succ_policy
        else:
            # if no more successor policy, set predecessor policy as final successor policy
            final_succ_policy = temp_pred_policy

    return final_succ_policy


def process_all_policies(args, p_pol_data_dict, p_pol_comb_dict, p_pol_comb_set, p_outfile, p_errfile):
    ''' 
    process all policies (combined and non-combined) and create a record for each one
    Only 2 tables are used in this query:policy and agreement, this will bring all the policies
    This query gets just the policy number for all policies (combined or not combined)
    '''

#    heading = (['pred_agre_id','pred_cust_id','pred_policy','pred_bus_seq','succ_agre_id','succ_cust_id','succ_policy','succ_bus_seq','create_date','update_date'])
#    out_rec = ','.join(heading)
#    p_outfile.write(out_rec + args.newline)

    sql = f'''
SELECT DISTINCT plcy_no 
  FROM {args.srcdb}.pcmp.policy_period 
 WHERE EXISTS 
      (SELECT * 
         FROM {args.srcdb}.pcmp.agreement 
        WHERE agre_typ_cd = 'plcy' 
          AND agre_id = policy_period.agre_id)
   AND void_ind = 'n'
 ORDER BY 1
          '''

    comb_process_count = non_comb_process_count = 0
    reject_count = 0

    for row in args.tgtcon.fetchdict(sql):
        #00000002 {'PLCY_NO': '00000002'}

        pred_policy = str(row['PLCY_NO']).zfill(8)
        
        if pred_policy in p_pol_comb_set:
            #this uses a while loop to process until final successor is found
            succ_policy = get_final_successor(pred_policy, p_pol_comb_dict)
            comb_process_count = comb_process_count + 1
        else:
            succ_policy = pred_policy
            non_comb_process_count = non_comb_process_count + 1

        err_rec = create_record(args,pred_policy, succ_policy, p_pol_data_dict, p_outfile, p_errfile)

        if err_rec:
            reject_count = reject_count + 1

    print('Total records processed: ' + str(comb_process_count + non_comb_process_count) + \
               ' (' + 'combined - ' + str(comb_process_count) + \
               ', non-combined - ' + str(non_comb_process_count) + ')')
    print('Total records  rejected: ' + str(reject_count))
    print('Total records   created: ' + str(comb_process_count + non_comb_process_count - reject_count) + \
               ' (header record not included in count)')
    return reject_count



def load_combined_data(args):
    ''' 
    get the original combination data and load them into a dictionary and a set
    The dictionary stored the immediate .The query below bring in all the policies that have been combined.
    This query gets those policies that have been combined into another
    '''

    pol_comb_dict = {} 
    pol_comb_set = set()

    sql = f'''
SELECT (b.fk_wept_nmbr::INTEGER * 10000000 + b.fk_wpsq_nmbr::INTEGER)::TEXT AS pred_policy, 
       (c.fk_wept_nmbr * 10000000 + c.fk_wpsq_nmbr) AS succ_policy
  FROM {args.srcdb}.bwcrnp.teaopps a, {args.srcdb}.bwcrnp.teawpcr b, {args.srcdb}.bwcrnp.teawpcr c
 WHERE a.fk_oeop_nmbr_p = b.fk_oeop_nmbr
   AND a.fk_oeop_nmbr_s = c.fk_oeop_nmbr
   AND a.dctvt_dttm > CURRENT_TIMESTAMP
   AND a.fk_optt_code <> 'PT'
  ORDER BY 1
          '''

    print('Executing sql: ' + sql)

    #{'PRED_POLICY': Decimal('6.00000'), 'SUCC_POLICY': Decimal('1029469.00000')}
    #06.00000 1029469.00000

    for row in args.tgtcon.fetchdict(sql):
        pred_policy = str(row['PRED_POLICY']).zfill(8)
        succ_policy = str(row['SUCC_POLICY']).zfill(8)

        pol_comb_dict[pred_policy] = succ_policy

        if pred_policy not in pol_comb_set:
            pol_comb_set.add(pred_policy)

    args.log.info(f'pol_comb_dict has {len(pol_comb_dict)} records')

    return pol_comb_dict, pol_comb_set



def load_policy_data(args):
    ''' 
    get the policy data and load them into a dictionary
    '''
    pol_data_load_count = 0

    pol_data_dict = {}  

    #join 2 tables on agre_id and filter for plcy rows which are not voided,brings all the population of the policies
   #This query gets associated data (policy number, agre_id & cust_id_acct_hldr) for all policies (combined or not combined)
    sql = f'''
SELECT DISTINCT pp.plcy_no, pp.agre_id, p.cust_id_acct_hldr 
  FROM {args.srcdb}.pcmp.policy_period pp, {args.srcdb}.pcmp.policy p, {args.srcdb}.pcmp.agreement a 
 WHERE pp.agre_id = p.agre_id 
   AND (pp.agre_id = a.agre_id 
   AND  a.agre_typ_cd = 'plcy') 
   AND  pp.void_ind = 'n' 
   AND  p.void_ind = 'n' 
          '''

    for row in args.tgtcon.fetchdict(sql):

        plcy_no = row['PLCY_NO']
        #perhaps to get rid of the .0 in 11111.0  if the id is treated as a float
        if '.' in str(row['CUST_ID_ACCT_HLDR']): print(row)
        agre_id = str(row['AGRE_ID']).split('.')[0]
        cust_id= str(row['CUST_ID_ACCT_HLDR']).split('.')[0]

        pol_data_dict[plcy_no] = (agre_id, cust_id)


    args.log.info(f'records extracted {len(pol_data_dict)}')

    return pol_data_dict

def create_table(args,dbname,schema,table):
    columns_str='''
        PLCY_AGRE_ID NUMBER(31,0),
        CUST_ID_ACCT_HLDR NUMBER(31,0),
        PLCY_NO TEXT,
        BSNS_SQNC_NO NUMBER(3,0) DEFAULT 0,
        EOC_PLCY_AGRE_ID NUMBER(31,0),
        EOC_CUST_ID_ACCT_HLDR NUMBER(31,0),
        EOC_PLCY_NO TEXT,
        EOC_BSNS_SQNC_NO NUMBER(3,0) DEFAULT 0,
        DW_CREATE_DTTM TIMESTAMP,
        DW_UPDATE_DTTM TIMESTAMP
    '''
    sql=f"""CREATE OR REPLACE  TABLE {dbname}.{schema}.{table} ({columns_str} );\n"""
    print(sql)
    result=list(args.tgtcon.exe(sql))[0]
    if 'successfully created' not in result['status']:
        raise Warning(f' did not create table {result}')


def process_args():
    '''
    '''

    #etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    transfer_dir=Path('//mswg9/groups/IT/ETL/TRANSFER/PD')

    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--srcdir', required=True, help='src dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
      #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    #optional
    parser.add_argument('--table',required=False, default='DW_PLCY_END_OF_CHAIN_CMBNS',help='table to load into')
    parser.add_argument('--error_file',required=False, default='error.txt ',help='default directory for all logs, data files')
    parser.add_argument('--output_file',required=False, default='run_get_final_succ_pol.txt', help='default directory for all logs, data files')
  
    parser.add_argument('--eldir', required=False, default=eldir,help='default directory for all logs, data files')
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    #
    args= parser.parse_args()

    args.root=Path(__file__).resolve().parent.parent
    #args.loaddir=args.root/'bwcpresent'

    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d%p') #2021_05_14AM
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key=ymd
    args.load_ts=ymd_hms

    inf.build_args_paths(args,use_load_key=True)
    
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')
    args.newline= '\n'

    return args


def main():
    try:
        args=None
        args=process_args()

        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
        args.tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = tgtdb_dict.get('port',''))
        
        data_dir= args.tgtdata
        error_file = data_dir/args.error_file
        output_file = data_dir/args.output_file

        tmp_file=data_dir/'tmp_endofchain.gz'
        final_file=data_dir/'endofchain.gz'
        #make a dict of all policies and their current agreid and custid
        #(policy,(agreid,custid) ->  ('37722204', ('2172251', '2172251'))
        pol_data_dict =load_policy_data(args)
        pol_comb_dict, pol_comb_set = load_combined_data(args) 

        #open files
        tmpfile=gzip.open(tmp_file,'wt',compresslevel=1)
        errfile = open(error_file, 'w')
        args.log.info('writing to gzip file:'+str(tmpfile))

        #This writes final results to be loaded into tmpfile
        #this calls another function that uses a while loop to process until final successor is found
        #SQL cannot do this
        reject_count=process_all_policies(args, pol_data_dict, pol_comb_dict, pol_comb_set, tmpfile, errfile)
        errfile.close();tmpfile.close()
        args.log.info('done writing to gzip file:'+str(tmpfile))
        #Path(args.final_dest.parent).mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tmp_file,final_file)
        #create_table(args,args.tgtdb,args.tgtschema,args.table)
        #args.tgtcon.load_file(args.tgtdb,args.tgtschema,args.table,tmp_file,delim=',')
        args.log.info(f'copied to {final_file}')

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
python run_get_final_succ_pol.py --srcdir /uat/snow_etl/rub1/pcmp --tgtdir /uat/snow_etl/rub1/DW_REPORT --eldir c:/temp
python run_get_final_succ_pol.py --srcdir /prd/snow_etl/rpd1/pcmp --tgtdir /prd/snow_etl/rpd1/DW_REPORT --eldir c:/temp
python run_get_final_succ_pol.py --srcdir /dev/snow_etl/rda1_source/pcmp --tgtdir /dev/snow_etl/rda1_source/DW_REPORT --eldir c:/temp

I:\IT\ETL\TRANSFER\PD\DW_REPORT\DW_PLCY_END_OF_CHAIN_CMBNS.txt


'''
