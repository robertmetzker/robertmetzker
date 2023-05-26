#!/usr/bin/env python
'''
USING:  PCMP.POLICY_CONTROL_ELEMENT, PCMP.CONTROL_ELEMENT_SUB_TYPE, DW_REPORT.DW_PLCY_CMBN, PCMP.PARTICIPATION, PCMP.POLICY_PERIOD_PARTICIPATION, 
        PCMP.POLICY_PERIOD_PTCP_INS, PCMP.POLICY, PCMP.POLICY_PERIOD, PCMP.POLICY_CONTROL_ELEMENT, PCMP.CONTROL_ELEMENT_SUB_TYPE, 
        PCMP.POLICY_STATUS, PCMP.POLICY_STATUS_REASON
        BWC_RPT.CNTL_DATES_BATCH_EL
        DW_PLCY_CMBN
'''

'''
=============================================================================================
NOTES: 
1) set debug flag to appropriate value before executing program
2) test this program in Linux via the following commands (using sample agre_id):
   a) mkdir /etl/dwdata/coxxxbd/policy_hist_unittest (necessary if directory does not exist)
   b) rm *policy*1055670* /etl/dwdata/coxxxbd/policy_hist_unittest/*policy*1055670*
   c) python run_policy_hist_vsql.py  --operation policy_hist  --s_environment dev_cognos  --srcdb vertica  --s_schema DW_REPORT  --t_environment dev_cognos  --tgtdb vertica  --t_schema DW_REPORT  --load_type PCMP  --topdir /etl/dwdata/coxxxbd/  --min_agid 7255962  --max_agid 7255962  --cache 1
   d) cp *policy*1055670* /etl/dwdata/coxxxbd/policy_hist_unittest (necessary for executing test_run_policy_hist_vsql.py program later)
   e) zcat policy_smush_1055670_1055670.csv.gz
3) test this program in Automic via the following commands:
   a) dzdo su - svc_etl
   b) cp /etl/prod_i_drive/IT/ETL/coxxxbd/tfs/M174218_coxxxbd/DevBranch/ETL/Python/run_policy_hist_vsql.py /usr/local/etl/run
   c) chmod 755 /usr/local/etl/run/run_policy_hist_vsql.py (to prevent "Permission denied" error)
   d) execute the JOBP.DW.POLICY.INSRD.PARTICIPATION.HISTORY workflow
4) after testing, execute validation queries to verify data in table
5) full test run times (in Dev)
   a) about 16 minutes (command line)
   b) about 35 minutes (Automic - with concurrent jobs)
6) per discussion with John on 10/22/18, it is okay to use pipe as delimeter for this program
7) per discussion with Kumar, since PS does not have edits, it is possible for new situations 
   to come up in the future and they will be handled at that time
=============================================================================================
'''

#std libraries #-----------

# import time,os,base64,datetime,time,sys,socket,gzip
# import shutil,copy,random,pprint,operator,collections,pprint

import argparse, datetime, gzip, os, sys, socket, time
from pathlib import Path

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
    
    pylibpath = root/f'Python/Python{pyversion}/site-packages'
    pylibpath2 = root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('INFO: using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib, inf, inf2
from bwcsetup import dbsetup

# set this flag to appropriate value... 
# debug = True

##### SQL START ################################################################################

def get_policy_type_sql( db ):
    '''
    get all unique policy type information from specified table
    '''

    sql = f'''
   select distinct 
            cest.ctl_elem_sub_typ_cd   AS   policy_type_code, 
            cest.ctl_elem_sub_typ_nm   AS   policy_type_name
     from {db}.PCMP.POLICY_CONTROL_ELEMENT pce
          inner join PCMP.CONTROL_ELEMENT_SUB_TYPE cest 
            on pce.ctl_elem_sub_typ_id = cest.ctl_elem_sub_typ_id
    where pce.ctl_elem_typ_cd = 'plcy_typ'
    order by 1
      '''

    return sql 


def get_combine_sql( db ):
    '''
    get all predecessor and successor relationships from policy combine table
    '''

    sql = f'''
    select 
          prdsr_plcy_agre_id
        , prdsr_plcy_no
        , prdsr_cust_id
        , prdsr_bsns_sqnc_no
        , prdsr_plcy_typ_code
        , sucsr_plcy_agre_id
        , sucsr_plcy_no
        , sucsr_cust_id
        , sucsr_bsns_sqnc_no
        , sucsr_plcy_typ_code
        , plcy_trnsf_typ_cd
        , plcy_trnsf_typ_nm
        , cmbn_efctv_date
        , date( crt_dtm )  AS   crt_date
      from {db}.DW_REPORT.DW_PLCY_CMBN
      '''

    return sql 


def get_main_sql( db, where ):
    '''
    The following is the Driver query for gathering policy participation data, with correlated subquery
    using the above driver sql, consolidate the Policy Period, determine & resolve Overlapping Participation instances, 
    gather the Policy Combine data., etc. 

    Notes for Policy Combines : Check the policy if ever combined into another policy and also check for complete chain 
                                of combines until the end of chain.
    *  these excludes PA to SI combines and SI to SI combines when BSN = 0 is not part of the predecessor policy.
    ==>  join Plcy_no to DW_PLCY_CMBN Predecessor Policy_no
          and BSN to Predecessor BSN 
          and Cust_id to Predecessor Cust id 
    and exclude PA to SI combines and SI to SI combines when BSN = 0 is not part of the predecessor policy 

    PARTICIPATION
    1.	one row per customer, policy period, and participation type ( participation id -- primary key)
    2.	join to POLICY_PERIOD_PARTICIPATION to filter out claims, participation id -- primary key
    3.	each participation id is linked to a different policy period
    4.	for each new policy period, get new record for each period

    POLICY_PERIOD_PARTICIPATION
    1.	one row for each participation record
    2.	used with participation to show relationship between customer and policy period
    3.	shows which record is linked to which period
    4.	has effective and ending dates
    5.	often match policy period unless they start or end mid year (hire employer rep in middle of period), open 2nd location mid year
    6.  excludes Flat Cancelled policies (PPP.PLCY_PRD_PTCP_EFF_DT <> PPP.PLCY_PRD_PTCP_END_DT)

    POLICY
    1.	one row per agre_id
    2.	overall policy information
    3.	master of agre_id, and origination date,  quotes (we don't want quotes)

    POLICY_PERIOD
    1.	multiple rows per policy, one row per period
    2.	defines policy period range (policy number is stored here)
    3.	a quote can have no policy periods and then isn't in policy table
    4.	dates that say policy period effective and ending dates (7/1 to 7/1)

    POLICY_CONTROL_ELEMENT 
    1.	multiple rows per policy period, one row for each control element on policy period
    2.	used to find policy types (7 or 8 types)
    3.	identity policy type  
    4.	element=employer leasing type, subtype = peo

    CONTROL_ELEMENT_SUB_TYPE
    1.	one for each policy control element (kinda like code table)
    2.	what it is: pes, pa, si, black lung -- specific 7 elements define the policy (rating elements define premium rates in diff table)

    POLICY_STATUS
    1.	one current status per policy period
    2.	defined at period level
    3.	3 status types: active, cancelled, expired
    4.	at the end of the policy period, old policy period automatically expired, new one active

    POLICY_STATUS_REASON
    1.	one current status reason per policy period
    2.	further defines what status is (status has limited values), why did something happen
    3.	active: renewed, new, re-instated, lapsed (still active), cancel reason (didn't pay, employer closed, combine)
    '''

    sql = f'''
    select p.AGRE_ID    as  PLCY_AGRE_ID,
           p.CUST_ID,
           NULL         as sucsr_cust_id,
           pp.PLCY_NO,
           pppi.PLCY_PRD_PTCP_INS_BUSN_SEQ_NO as BUSN_SEQ_NO,
           DATE( ppp.PLCY_PRD_PTCP_EFF_DT ) as PLCY_PTCP_EFF_DATE,
           DATE( ppp.PLCY_PRD_PTCP_END_DT ) as PLCY_PTCP_END_DATE,
           cest.CTL_ELEM_SUB_TYP_CD   as   PLCY_TYP_CD,
           cest.CTL_ELEM_SUB_TYP_NM   as   PLCY_TYP_NM,
           PLCY_STS_TYP_CD,
           PLCY_STS_RSN_TYP_CD,
           CASE 
              WHEN ppp.PLCY_PRD_PTCP_END_DT IS NULL or (CURRENT_TIMESTAMP between PPP.PLCY_PRD_PTCP_EFF_DT and PPP.PLCY_PRD_PTCP_END_DT)  THEN 'y' 
              ELSE 'n' 
           END as CRNT_PLCY_PTCP_IND,
           CURRENT_DATE()  as  DW_CREATE_DTTM, 
           CURRENT_DATE()  as  DW_UPDATE_DTTM
      from {db}.PCMP.PARTICIPATION P
           inner join {db}.PCMP.POLICY_PERIOD_PARTICIPATION ppp 
                on p.PTCP_ID = ppp.PTCP_ID and p.PTCP_TYP_CD = 'insrd' and ppp.VOID_IND = 'n'
           inner join {db}.PCMP.POLICY_PERIOD_PTCP_INS pppi 
                on p.PTCP_ID = pppi.PTCP_ID
           inner join {db}.PCMP.POLICY py 
                on p.AGRE_ID = py.AGRE_ID and py.QUOT_NO is null and py.VOID_IND = 'n'
           inner join {db}.PCMP.POLICY_PERIOD pp 
                on ppp.PLCY_PRD_ID = pp.PLCY_PRD_ID and pp.VOID_IND = 'n'
           inner join {db}.PCMP.POLICY_CONTROL_ELEMENT pce 
                on pp.PLCY_PRD_ID = pce.PLCY_PRD_ID and pce.CTL_ELEM_TYP_CD = 'plcy_typ' and pce.VOID_IND = 'n'
           inner join {db}.PCMP.CONTROL_ELEMENT_SUB_TYPE cest 
                on pce.CTL_ELEM_SUB_TYP_ID = cest.CTL_ELEM_SUB_TYP_ID and cest.CTL_ELEM_SUB_TYP_VOID_IND = 'n'
           inner join {db}.PCMP.POLICY_STATUS PS 
                on PS.PLCY_PRD_ID = pp.PLCY_PRD_ID and PS.VOID_IND = 'n'
           inner join {db}.PCMP.POLICY_STATUS_REASON PSR 
                on PSR.PLCY_STS_ID = PS.PLCY_STS_ID and PSR.VOID_IND = 'n'
    {where}
       and ppp.PLCY_PRD_PTCP_EFF_DT <> ppp.PLCY_PRD_PTCP_END_DT  -- exclude Flat Cancelled policies
     order by p.AGRE_ID, pp.PLCY_NO, pppi.PLCY_PRD_PTCP_INS_BUSN_SEQ_NO, ppp.PLCY_PRD_PTCP_EFF_DT, ppp.PLCY_PRD_PTCP_END_DT
         '''

    return sql

##### SQL END ##################################################################################################


def get_column_order():
    '''
    get a list of columns that will be used in creating the file
    '''

#    col_order=['PLCY_AGRE_ID','CUST_ID','PLCY_NO','BUSN_SEQ_NO','PLCY_PTCP_EFF_DATE','PLCY_PTCP_END_DATE','PLCY_TYP_CD',
#     'PLCY_TYP_NM','PLCY_STS_TYP_CD','PLCY_STS_RSN_TYP_CD','CRNT_PLCY_PTCP_IND','DW_CREATE_DTTM','DW_UPDATE_DTTM']
    col_order = ['PLCY_AGRE_ID', 'CUST_ID', 'SUCSR_CUST_ID', 'PLCY_NO', 'BUSN_SEQ_NO', 'PLCY_PTCP_EFF_DATE', 'PLCY_PTCP_END_DATE', 'PLCY_TYP_CD',
     'PLCY_TYP_NM', 'PLCY_STS_TYP_CD', 'PLCY_STS_RSN_TYP_CD', 'CRNT_PLCY_PTCP_IND', 'DW_CREATE_DTTM', 'DW_UPDATE_DTTM']

    return col_order


def rowsdict2file(rows, fname, delim='\t', col_order=[]):
    '''
    This function produces a compressed csv file to be used by python for processing.
    The target table uses INS_PARTICIPANT instead of CUST_ID
    The column_order for the csv is determined by function get_column_order, called before
    this function.
    Initially a tmp file is written to and renamed only when completed.
    This means that if the job is terminated, the original extract is untouched.
    '''

    # fname=data_dir+'table_data_CLAIM_POLICY_HISTORY.gz'
    fname_tmp = fname + '.tmp'
    if os.path.exists( fname_tmp ): os.remove( fname_tmp )
    if os.path.exists( fname ): os.remove( fname )

    fw = gzip.open( fname_tmp, 'wb', compresslevel=1 )
    # fw.write('\t'.join(col_order)+'\n')
    output_rows = []
    print('-- writing', fname, len(rows))
    #now=datetime.datetime.now()

    for row in rows:
        output_row=[]

        for col in col_order:
            colval = row[col]
            if isinstance(colval, datetime.date) or isinstance(colval, datetime.datetime):
                if col.endswith('DATE'):
                    colval = colval.strftime('%Y-%m-%d')
                else:
                    colval = colval.strftime('%Y-%m-%d %H:%M:%S.%f')

            output_row.append(colval)

        final_row = delim.join(output_row)
        fw.write(final_row + '\n')

    fw.close()

    os.rename(fname_tmp, fname)
    print('--- finished data file', fname, time.asctime())

    return fname


def print_rows(rows, scol, ecol, idcol1='', idcol2='', mark=''):

    mark_loc = 0
    for idx, row in enumerate(rows):
        if mark == row:
            break

    delta = 10
    begin = idx - delta
    if begin < 0: begin = 0
    end = idx + delta
    if end > len(rows): end = len(rows)

    for row in rows[begin:end]:
        #print(sorted(row.keys()))
        #print(idcol1,idcol2,scol,ecol)
        markit = ''
        if mark == row:
            markit = '*'

        print(markit, row.get(idcol1), row.get(idcol2), row[scol], row[ecol])


# db_batch_row['PLCY_TYP_CD'] is used to determine policy type instead of this function
def get_policy_type(row):
    '''
    Policy numbers with 8 digits
        1   PES         10058100
        2   SI          20058100
        3   PEC         30058100
        4   ?   
        5   ?
        6   black lung  60058100
        7   marine fund 70058100

    Policy numbers with < 8 digits
        1   PA          1005810
    '''

    if len(row['PLCY_NO']) == 8:
        if row['PLCY_NO'][0] == '1':
            return 'PES'
        elif row['PLCY_NO'][0] == '2':
            return 'SI'
        elif row['PLCY_NO'][0] == '3':
            return 'PEC'
        elif row['PLCY_NO'][0] == '6':
            return 'BL'
        elif row['PLCY_NO'][0] == '7':
            return 'MF'
        else:
            raise ValueError("!! ERR: Unknown policy type").with_traceback(str(row))

    return 'PA'


def display_debugging_message(debug_msg):
    '''
    display specified debugging message
    '''

    print('>>> ' + debug_msg + '\n')


def check_last_row_of_new_batch(args, new_batch, ecol, combine_code):
    '''
    for the last row in new_batch list, set end_date = high_date and CRNT_PLCY_PTCP_IND = "y" as necessary
    '''

    curr_date = datetime.datetime.today()
    high_date = datetime.datetime.strptime('12/31/9999', '%m/%d/%Y')

    last_row_of_new_batch = new_batch[-1]

    # Values for combine_code: 0 - no combination, 1 - valid combination, 2 - invalid combination
    # if a policy is not combined or in a valid combination, continue processing;
    # otherwise, if a policy is in an invalid combination, set end_date to high date if it is <= current date
    if combine_code == 0 or combine_code == 1:
        pass
    else:
        if last_row_of_new_batch[ecol] <= curr_date:
            debug_msg = 'scenario 1a,2a,14b,16a,20,22,23,26,28 - setting end date of last row to high date:\n' + 'last_row_of_new_batch[ecol] <= curr_date'
            if args.debug: display_debugging_message(debug_msg)

            last_row_of_new_batch[ecol] = high_date

    # if the following criteria are met then set indicator to 'y'
    if last_row_of_new_batch[ecol] > curr_date:
        if last_row_of_new_batch['CRNT_PLCY_PTCP_IND'] != 'y':
            debug_msg = 'scenario 9b,17,22,30,32 - setting CRNT_PLCY_PTCP_IND to "y" in  last row:\n' + 'last_row_of_new_batch[ecol] > curr_date and last_row_of_new_batch[CRNT_PLCY_PTCP_IND] != y'
            if args.debug: display_debugging_message(debug_msg)

            last_row_of_new_batch['CRNT_PLCY_PTCP_IND'] = 'y'


def get_policy_type_name(policy_type_code, policy_type_lookup_dict):
    '''
    for specified policy type code, get corresponding policy type name in lookup dictionary
    '''

    row_key = policy_type_code

    # print('in get_policy_type_name(), looking for policy_type_code: ' + row_key + ' in policy_type_lookup_dict: ' + str(policy_type_lookup_dict))

    if row_key in policy_type_lookup_dict:
        policy_type_name_dict = dict(policy_type_lookup_dict[row_key])
        # print('in get_policy_type_name(), policy_type_name_dict: ' + str(policy_type_name_dict))
        policy_type_name = policy_type_name_dict['POLICY_TYPE_NAME']
    else:
        policy_type_name = 'N/A'

    return policy_type_name


def create_succ_pol_row(args, db_batch_row, last_row_of_new_batch, new_batch, first_successor_row_ind, succ_pol_row_dict, policy_type_lookup_dict, aday):
    '''
    create a new row for successor policy

    Business rule:
    if CRT_DATE > create cutoff date and CMBN_EFCTV_DATE > combine cutoff date
        if previous successor policy have same CRT_DATE
            delete that row
        else
            continue processing
    else
        use CMBN_EFCTV_DATE
    '''

    high_date = datetime.datetime.strptime('12/31/9999', '%m/%d/%Y')
    create_cutoff_date = datetime.datetime.strptime('09/17/2000', '%m/%d/%Y')
    combine_cutoff_date = datetime.datetime.strptime('12/31/1997', '%m/%d/%Y')

    date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f' ]

    for field, val in list( succ_pol_row_dict.items() ):
        if '_DATE' in field or '_DTM' in field:
            if not isinstance( val, datetime.datetime ):
                for df in date_formats:
                    try:
                        val = datetime.datetime.strptime( val, df )
                        break
                    except ValueError:
                        pass
                else:
                    val = val
            succ_pol_row_dict[ field ] = val    

    if succ_pol_row_dict['CRT_DATE'] > create_cutoff_date and succ_pol_row_dict['CMBN_EFCTV_DATE'] > combine_cutoff_date:
        combine_eff_date = succ_pol_row_dict['CRT_DATE']
    else:
        combine_eff_date = succ_pol_row_dict['CMBN_EFCTV_DATE']

    # print('in create_succ_pol_row(), create_cutoff_date: ' + create_cutoff_date.strftime('%m/%d/%Y') + ', succ_pol_row_dict[CRT_DATE]: ' + succ_pol_row_dict['CRT_DATE'].strftime('%m/%d/%Y') + ', succ_pol_row_dict[CMBN_EFCTV_DATE]: ' + succ_pol_row_dict['CMBN_EFCTV_DATE'].strftime('%m/%d/%Y') + ', combine_eff_date: ' + combine_eff_date.strftime('%m/%d/%Y'))
    # print('in create_succ_pol_row(), first_successor_row_ind: ' + first_successor_row_ind + '\n' + 'db_batch_row: ' + str(db_batch_row) + '\n' + 'last_row_of_new_batch[PLCY_PTCP_END_DATE]: ' + str(last_row_of_new_batch['PLCY_PTCP_END_DATE']) + '\n' + 'succ_pol_row_dict[CMBN_EFCTV_DATE]: ' + str(succ_pol_row_dict['CMBN_EFCTV_DATE']))

    if first_successor_row_ind == 'y':
        new_batch_index = 0

        # ******************** start of FOR loop ********************
        if len(new_batch) <= 1:
            debug_msg = 'scenario 28 - SI_policy combined with end_date > combine_date but only 1 row in new_batch:\n' + \
                        'not deleting previous row in new_batch'
            if args.debug: display_debugging_message(debug_msg)
        else:
            for new_batch_row in new_batch[0:]:
                # delete all rows whose ending date is after combine date            
                if new_batch_row['PLCY_PTCP_END_DATE'] > combine_eff_date:
                    del new_batch[new_batch_index]
                    new_batch_index -= 1
                else:
                    new_batch_index += 1
        # ********************   end of FOR loop ********************

        last_row_of_new_batch = new_batch[-1]
        # print('in create_succ_pol_row(), last_row_of_new_batch:\n' + str(last_row_of_new_batch))

    last_row_of_new_batch['PLCY_PTCP_END_DATE'] = combine_eff_date - aday
    last_row_of_new_batch['CRNT_PLCY_PTCP_IND'] = 'n'

    succ_pol_row = dict(db_batch_row)

    # populate specified fields below with appropriate values
    succ_pol_row['PLCY_AGRE_ID']            = db_batch_row['PLCY_AGRE_ID']
    succ_pol_row['CUST_ID']                 = db_batch_row['CUST_ID']
    succ_pol_row['SUCSR_CUST_ID']           = succ_pol_row_dict['SUCSR_CUST_ID']
    succ_pol_row['PLCY_NO']                 = succ_pol_row_dict['SUCSR_PLCY_NO']
    succ_pol_row['BUSN_SEQ_NO']             = succ_pol_row_dict['SUCSR_BSNS_SQNC_NO']
    succ_pol_row['PLCY_PTCP_EFF_DATE']      = combine_eff_date
    succ_pol_row['PLCY_PTCP_END_DATE']      = high_date
    succ_pol_row['PLCY_TYP_CD']             = succ_pol_row_dict['SUCSR_PLCY_TYP_CODE']
    succ_pol_row['PLCY_TYP_NM']             = get_policy_type_name(succ_pol_row['PLCY_TYP_CD'], policy_type_lookup_dict)
    succ_pol_row['PLCY_STS_TYP_CD']         = ''
    succ_pol_row['PLCY_STS_RSN_TYP_CD']     = ''
    succ_pol_row['CRNT_PLCY_PTCP_IND']      = 'y'

    # for multiple combines occurring on the same day, keep the latest one by deleting the previous (last) row in new_batch
    if last_row_of_new_batch['PLCY_PTCP_EFF_DATE'] == succ_pol_row['PLCY_PTCP_EFF_DATE']:
        debug_msg = 'scenario 27 - deleting older combines on same day:\n' + 'last_row_of_new_batch[PLCY_PTCP_EFF_DATE] == succ_pol_row[PLCY_PTCP_EFF_DATE]'
        if args.debug: display_debugging_message(debug_msg)

        del new_batch[-1]

    return succ_pol_row


def iscombined(row_dict, combine_lookup_dict):
    '''
    check if a policy has been combined into another
    (we assume row_dict['CUST_ID'] changes with each combine)
    '''

    if 'PLCY_AGRE_ID' in row_dict:
        row_key = (row_dict['PLCY_AGRE_ID'], row_dict['PLCY_NO'], row_dict['BUSN_SEQ_NO'], row_dict['CUST_ID'])
    else:
        row_key = (row_dict['PRDSR_PLCY_AGRE_ID'], row_dict['PRDSR_PLCY_NO'], row_dict['PRDSR_BSNS_SQNC_NO'], row_dict['PRDSR_CUST_ID'])

    # print('in iscombined(), getting combine data for row_key: ' + str(row_key))

    combined_row_dict = {}

    if row_key in combine_lookup_dict:
        combined_row_dict = dict(combine_lookup_dict[row_key])
    else:
        pass  # not combined

    # print('in iscombined(), combined_row_dict: ' + str(combined_row_dict))

    return combined_row_dict


def get_final_successor(args, db_batch_row, policy_type_lookup_dict, combine_lookup_dict, add_succ_pol_rows_ind, last_row_of_new_batch, new_batch, aday):

    successor_row_dict = dict(db_batch_row)
    iteration = 0
    combine_code = 0  # 0 - no combination, 1 - valid combination, 2 - invalid combination

    # print('in get_final_sucessor(), row_dict[PLCY_AGRE_ID]=' + row_dict['PLCY_AGRE_ID'] + ', row_dict[PLCY_NO]=' + row_dict['PLCY_NO'] + ', row_dict[CUST_ID]=' + row_dict['CUST_ID'] + ', row_dict[BUSN_SEQ_NO]=' + row_dict['BUSN_SEQ_NO'])

    while True:
        iteration += 1

        combine_row_dict = iscombined(successor_row_dict, combine_lookup_dict)

        # stop further processing if policy is not combined
        if len(combine_row_dict) == 0: 
            if iteration == 1:
                successor_row_dict = {}     # declare an empty dictionary if there is no combination

            break
            
        successor_row_dict = combine_row_dict

        combine_code = 1  # set value to valid combination by default unless overridden below

        # skip further processing if invalid combinations are found
        if (successor_row_dict['PRDSR_PLCY_TYP_CODE'] == 'pa' and successor_row_dict['SUCSR_PLCY_TYP_CODE'] == 'si') or \
           (successor_row_dict['PRDSR_PLCY_TYP_CODE'] == 'si' and successor_row_dict['PRDSR_BSNS_SQNC_NO'] != '0'): 
            combine_code = 2            # set value to invalid combination
            successor_row_dict = {}     # declare an empty dictionary if there is no combination

            break

        if add_succ_pol_rows_ind == 'n':
            # when dealing with gap rows, no need to get data for each successor policy since the goal is just to get the final successor policy's type ('si' or not)
            pass
        else:
            if iteration == 1:
                first_successor_row_ind = 'y'
            else:
                first_successor_row_ind = 'n'

            succ_pol_row = create_succ_pol_row(args, db_batch_row, last_row_of_new_batch, new_batch, first_successor_row_ind, successor_row_dict, policy_type_lookup_dict, aday)
            new_batch.append(succ_pol_row)
            last_row_of_new_batch = new_batch[-1]
            # print('in get_final_successor(), added successor policy data: ' + str(succ_pol_row))

        # set successor policy as the predecessor for next look up
        successor_row_dict['PRDSR_PLCY_AGRE_ID'] = successor_row_dict['SUCSR_PLCY_AGRE_ID']
        successor_row_dict['PRDSR_PLCY_NO']      = successor_row_dict['SUCSR_PLCY_NO']
        successor_row_dict['PRDSR_BSNS_SQNC_NO'] = successor_row_dict['SUCSR_BSNS_SQNC_NO']
        successor_row_dict['PRDSR_CUST_ID']      = successor_row_dict['SUCSR_CUST_ID']

        # abort if iteration count is too high since it may mean the program is in an infinite loop
        if iteration > 999: 
            raise ValueError("Too many iterations in get_final_sucessor, please investigate!")

    # print('in get_final_sucessor(), combine_code = ' + str(combine_code) + ' and successor_row_dict = ' + str(successor_row_dict))

    return combine_code, successor_row_dict


def create_gap_row(db_batch_row, last_row_of_new_batch, scol, ecol, aday):
    '''
    create a new row to fill in the gap between 2 date periods
    '''

    dummy_cust_id = '1000042034'

    gap_row = dict(last_row_of_new_batch)

    # populate specified fields below with appropriate values
    gap_row['PLCY_AGRE_ID']         = ''
    gap_row['CUST_ID']              = db_batch_row['CUST_ID']
    gap_row['SUCSR_CUST_ID']        = dummy_cust_id
    gap_row['PLCY_NO']              = ''
    gap_row['BUSN_SEQ_NO']          = ''
    gap_row['PLCY_PTCP_EFF_DATE']   = last_row_of_new_batch[ecol] + aday
    gap_row['PLCY_PTCP_END_DATE']   = db_batch_row[scol] - aday
    gap_row['PLCY_TYP_CD']          = ''
    gap_row['PLCY_TYP_NM']          = ''
    gap_row['PLCY_STS_TYP_CD']      = ''
    gap_row['PLCY_STS_RSN_TYP_CD']  = ''

    return gap_row


def merge_prev_and_curr_rows(last_row_of_new_batch, db_batch_row, ecol):
    '''
    merge current into previous row
    '''

    last_row_of_new_batch[ecol]                     = db_batch_row[ecol]
    last_row_of_new_batch['PLCY_STS_TYP_CD']        = db_batch_row['PLCY_STS_TYP_CD']
    last_row_of_new_batch['PLCY_STS_RSN_TYP_CD']    = db_batch_row['PLCY_STS_RSN_TYP_CD']
    last_row_of_new_batch['CRNT_PLCY_PTCP_IND']     = db_batch_row['CRNT_PLCY_PTCP_IND']


def smush(args, starting_index, db_batch, policy_type_lookup_dict, combine_lookup_dict, scol, ecol, idcol1='', idcol2='', log_obj=None):
    '''
    If there is a gap between 2 periods, must use the following rules: (One Cust ID With more than one policy)
    1)  for non-SI policies (predecessor bus seq = 0)
		a)  if not combined, do not add a gap row (claim should retain on the policy)
        b)  if combined
            1)  if successor policy is Non-SI, add a gap row
            2)  if successor policy is SI, do not add a gap row (retain the non-SI policies - claims will not be moved)
		
    2)  for SI policies
        a)  if not combined, add a gap row with BSN-0 ptcp details (Kumar will check with business)
        b)  if combined
            1)  if predecessor bus seq = 0, add a gap row
            2)  if predecessor bus seq != 0 and successor bus seq = 0, do not add a gap row  (claim remained on Predecessor)
            3)  if predecessor bus seq != 0 and successor bus seq != 0, do not add a gap row (claim remained on Predecessor)

    Additional Filters : - Policy Combines with changes in Control Element Sub type; 													
    1)  When the PA Predecessor policy combined into to SI, then all the claims remained at the PA policy
    2)  when PA to PA Combines then the claims get moved to Successor policy													
    3)  when SI combined into SI policy, Claims only moves when the BSN - 0 is part of the combine
													
    Email from Teresa													
    The below info was provided by John Jester.  													
     Claims only moves when the BSN - 0 is part of the combine.  
     If BSN -0 in the predecessor policy is not part of the combine then no claims move. 
     If BSN -0 is part of the combine then all claims automatically move to successor BSN-0 policy.													
													
     Example: 
     Predecessor BSN-0,-1 and -2 are all combined into Successor BSN-1,-2 and -3.  All claims from Predecessor are moved to Successor BSN-0 -- Kumar will check with business
     Claims will then need to be manually reassigned to the correct BSN #s in the successor policy													
    '''

    aday = datetime.timedelta(days=1)
    new_batch = []

    # add first item in current batch to new_batch list
    date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f' ]

    # add first row in current batch to new_batch list
    for field, val in list( db_batch[0].items()) :
        if '_DATE' in field or '_DTTM' in field or '_DTM' in field:
            if not isinstance( val, datetime.datetime ):
                for df in date_formats:
                    try:
                        val = datetime.datetime.strptime( val, df )
                        break
                    except ValueError:
                        pass
                else:
                    val = val
            db_batch[0][ field ] = val 
    new_batch.append(db_batch[0])  

    # ******************** start of FOR loop ********************
    for db_batch_row in db_batch[starting_index:]:
        debug_msg = ''
        error_message = ''

        # Loop through the values and try to convert it to a datetime
        for field, val in list( db_batch_row.items() ):
            if '_DATE' in field or '_DTM' in field:
                if not isinstance( val, datetime.datetime ):
                    for df in date_formats:
                        try:
                            val = datetime.datetime.strptime( val, df )
                            break
                        except ValueError:
                            pass
                    else:
                        val = val
                db_batch_row[ field ] = val            

        # this statement is used for debugging when this program aborts 
        # print('processing agre_id: ' + str(db_batch_row['PLCY_AGRE_ID']) + ', cust_id: ' + str(db_batch_row['CUST_ID']) + ', plcy_no: ' + str(db_batch_row['PLCY_NO']) + ', bus_seq: ' + str(db_batch_row['BUSN_SEQ_NO']) + ', plcy_ptcp_eff_date: ' + str(db_batch_row['PLCY_PTCP_EFF_DATE']))

        last_row_of_new_batch = new_batch[-1]

        # print('in smush():')
        # print('last_row_of_new_batch: \n' + str(last_row_of_new_batch))
        # print('db_batch_row: \n' + str(db_batch_row))
        # print('db_batch_row[scol]: ' + str(db_batch_row[scol]) + ', db_batch_row[ecol]: ' + str(db_batch_row[ecol]) + ', last_row_of_new_batch[scol]: ' + str(last_row_of_new_batch[scol]) + ', last_row_of_new_batch[ecol]: ' + str(last_row_of_new_batch[ecol]))
        # print()

        #-----------------skip current row (inside date range of previous row)
        #  leff     lecol   last_row_of_new_batch
        #  ceff     cecol   db_batch_row
        if last_row_of_new_batch[ecol] >= db_batch_row[ecol]: 
            if last_row_of_new_batch['BUSN_SEQ_NO'] == db_batch_row['BUSN_SEQ_NO']:
                debug_msg = 'scenario 22,24 - skipping current row:\n' + 'last_row_of_new_batch[ecol] >= db_batch_row[ecol] AND last_row_of_new_batch[BUSN_SEQ_NO] == db_batch_row[BUSN_SEQ_NO]'
                if args.debug: display_debugging_message(debug_msg)

                continue
            else:
                debug_msg = 'scenario 24 - NOT skipping current row:\n' + 'last_row_of_new_batch[ecol] >= db_batch_row[ecol] AND last_row_of_new_batch[BUSN_SEQ_NO] != db_batch_row[BUSN_SEQ_NO]'
                
                pass

        #-----------------merge current row into previous row
        #  leff     lecol [aday]           last_row_of_new_batch
        #           ceff           cecol   db_batch_row
        elif last_row_of_new_batch[ecol] + aday >= db_batch_row[scol]:
            # merge the rows if the business sequence number is the same for the previous and current row
            if last_row_of_new_batch['BUSN_SEQ_NO'] == db_batch_row['BUSN_SEQ_NO']:
                debug_msg = 'scenario 1a,1b,2a,2b,3a,3b,4a,4b,4c,5a,5b,5c,5d,6,7a,7b,7c,7d,8a,8b,8c,9a,9b,9c,10a,10b,10c,10d,10e,' + \
                            '14,16a,16b,16c,17,18,20,21,22,23,24,25,26 - merging previous and current rows:\n' + \
                            'last_row_of_new_batch[ecol] + aday >= db_batch_row[scol] AND last_row_of_new_batch[BUSN_SEQ_NO] == db_batch_row[BUSN_SEQ_NO]'
                if args.debug: display_debugging_message(debug_msg)
                
                merge_prev_and_curr_rows(last_row_of_new_batch, db_batch_row, ecol)

                continue
            else:
                debug_msg = 'scenario ?? - NOT merging previous and current rows:\n' + \
                            'last_row_of_new_batch[ecol] + aday >= db_batch_row[scol] AND last_row_of_new_batch[BUSN_SEQ_NO] != db_batch_row[BUSN_SEQ_NO]'

                pass

        #-----------------add current row (and a gap row if necessary)
        #  leff     lecol [aday]                         last_row_of_new_batch
        #                         ceff           cecol   db_batch_row
        elif last_row_of_new_batch[ecol] + aday < db_batch_row[scol]:
            add_succ_pol_rows_ind = 'n'
            combine_code, combined_row_dict = get_final_successor(args, db_batch_row, policy_type_lookup_dict, combine_lookup_dict, add_succ_pol_rows_ind, last_row_of_new_batch, new_batch, aday)
            # print('in smush(), combined_row_dict: ' + str(combined_row_dict) + '\n')

            # if get_policy_type(db_batch_row) == 'SI':
            if db_batch_row['PLCY_TYP_CD'] == 'si':
                if len(combined_row_dict) == 0:
                    debug_msg = 'scenario 6,17,18,20,23,26 - adding gap row for non-combined SI:\n' + 'db_batch_row[PLCY_TYP_CD] == "si"'

                    gap_row = create_gap_row(db_batch_row, last_row_of_new_batch, scol, ecol, aday)
                    new_batch.append(gap_row)

                    # added special code for scenario #25 to populate eff date & end date for successor policies with correct values in create_succ_pol_row function (after adding gap row)
                    last_row_of_new_batch = db_batch_row
                else:
                    if db_batch_row['BUSN_SEQ_NO'] == '0':
                        debug_msg = 'scenario ?? - adding gap row for combined SI - SEQ 0:\n' + 'db_batch_row[PLCY_TYP_CD] == "si" AND db_batch_row[BUSN_SEQ_NO] = "0"'

                        gap_row = create_gap_row(db_batch_row, last_row_of_new_batch, scol, ecol, aday)
                        new_batch.append(gap_row)

                        # added special code for scenario #25 to populate eff date & end date for successor policies with correct values in create_succ_pol_row function (after adding gap row)
                        last_row_of_new_batch = db_batch_row
                    else:
                        debug_msg = 'scenario 20,23,24 - NOT adding gap row for combined SI - SEQ non-0:\n' + 'db_batch_row[PLCY_TYP_CD] == "si" AND db_batch_row[BUSN_SEQ_NO] != "0"'
                
                        pass
            else:
                if len(combined_row_dict) == 0:
                    debug_msg = 'scenario 21 - not adding gap row for non-combined non-SI BUT setting end_date of previous row to eff_date of current row - 1 day:\n' + 'db_batch_row[PLCY_TYP_CD] != "si"'
            
                    last_row_of_new_batch['PLCY_PTCP_END_DATE'] = db_batch_row[scol] - aday
                else:
                    if combined_row_dict['SUCSR_PLCY_TYP_CODE'] == 'si':
                        debug_msg = 'scenario ?? - NOT adding gap row for combined non-SI to SI:\n' + 'db_batch_row[PLCY_TYP_CD] != "si" AND combined_row_dict[SUCSR_PLCY_TYP_CODE] == "si"'

                        pass
                    else:
                        debug_msg = 'scenario 25 - adding gap row for combined non-SI to non-SI:\n' + 'db_batch_row[PLCY_TYP_CD] != "si" AND combined_row_dict[SUCSR_PLCY_TYP_CODE] != "si"'

                        gap_row = create_gap_row(db_batch_row, last_row_of_new_batch, scol, ecol, aday)
                        new_batch.append(gap_row)

                        # added special code for scenario #25 to populate eff date & end date for successor policies with correct values in create_succ_pol_row function (after adding gap row)
                        last_row_of_new_batch = db_batch_row

        else:
            # display message but do not abort since it will be identified by validation queries and appropriate action (i.e. data fix) will be taken afterwards 
            error_msg = '### ERROR: unexpected situation:\n' + 'last_row_of_new_batch:\n' + str(last_row_of_new_batch) + 'db_batch_row:\n' + str(db_batch_row) + '\n'
            log_obj.info(error_msg)
            print(error_msg)
            print()

        if args.debug: 
            display_debugging_message(debug_msg)

        # add item in current batch to new_batch list
        new_batch.append(db_batch_row)

        # print('in smush(), new_batch at end of loop: ' + str(new_batch) + '\n')
    # ******************** end of FOR loop ********************

    # at end of each batch, check if policy is combined; if so, add row for successor policy in new_batch list
    # print('in smush() at end of batch, check if policy is combined for db_batch: \n' + str(db_batch_row))
    add_succ_pol_rows_ind = 'y'
    combine_code, combined_row_dict = get_final_successor(args, db_batch_row, policy_type_lookup_dict, combine_lookup_dict, add_succ_pol_rows_ind, last_row_of_new_batch, new_batch, aday)

    check_last_row_of_new_batch( args, new_batch, ecol, combine_code)

    return new_batch


def get_batch( rows, col ):
    '''
    IMPORTED from vertica legacy inf_vsql
    row generator: pec_pes_rows,column:'CLM_AGRE_ID'
    batch only has 1 element in it at most
    example batch = AGRE_ID:[ row, row, row ]
    '''

    batch = {}
    for row in rows:
        if not isinstance(row, dict): 
            raise ValueError('# ERR: row must be a dict')
        col_value = row[col]
        if col_value not in batch:
            if batch:
                #got new key, yield rows from old key
                old_rows = batch.popitem()[1]
                yield old_rows
                #batch is now empty
            batch[col_value] = []
        #always runs
        batch[col_value].append(row)    
    if batch:
        yield batch.popitem()[1]
    #print 'batch',batch


def process_batch(args, db_batch, policy_type_lookup_dict, combine_lookup_dict, scol, ecol, idcol1='', idcol2='', log_obj=None):
    '''
    ORDER BY P.AGRE_ID, PP.PLCY_NO, PPPI.PLCY_PRD_PTCP_INS_BUSN_SEQ_NO, PPP.PLCY_PRD_PTCP_EFF_DT, PPP.PLCY_PRD_PTCP_END_DT
    '''

    if len(db_batch) == 1:
        starting_index = 0
    else:
        starting_index = 1
        
    new_batch = smush(args, starting_index, db_batch, policy_type_lookup_dict, combine_lookup_dict, scol, ecol, idcol1, idcol2, log_obj)

    # print('in process_batch(), db_batch:' + '\n' + str(db_batch))
    # print('in process_batch(), new_batch:' + '\n' + str(new_batch))

    return new_batch


def process_dbrows(args, rows, policy_type_lookup_dict, combine_lookup_dict, log_obj=None):
    '''
    rows are ordered by order by AGRE_ID, CUST_ID, PLCY_PRD_PTCP_EFF_DATE, PLCY_PRD_PTCP_END_DATE

    1000439 1000439 1964-07-01 00:00:00 2017-12-07 00:00:00
    '''

    #for row in rows: print(sorted(row.keys()));break
    final_rows = []

    batches         =  get_batch(rows, 'PLCY_AGRE_ID')
    start           = 'PLCY_PTCP_EFF_DATE'
    end             = 'PLCY_PTCP_END_DATE'
    idcol1          = 'CUST_ID'
    idcol2          = 'PLCY_AGRE_ID'

    for rowid, agreid_batch in enumerate(batches):
        for custid_batch in get_batch(agreid_batch, 'CUST_ID'):
            new_batch = process_batch(args, custid_batch, policy_type_lookup_dict, combine_lookup_dict, start, end, idcol1, idcol2, log_obj)
            final_rows += new_batch
            # print('\n' + 'in process_dbrows(), new_batch:' + '\n' + str(new_batch) + '\n')

        #print(len(batch),len(new_batch))

    # print('in process_dbrows(), final_rows:' + '\n' + str(final_rows))

    return final_rows


def build_combine_lookup_dict(combine_rows, log_obj=None):
    ''' 
    build a lookup dictionary for policy combinations
    '''

    combine_lookup_dict = {}

    for row in combine_rows:
        agid        = row['PRDSR_PLCY_AGRE_ID']
        plcyno      = row['PRDSR_PLCY_NO']
        seqno       = row['PRDSR_BSNS_SQNC_NO']
        custid      = row['PRDSR_CUST_ID'] 
        akey        = (agid, plcyno, seqno, custid)

        if akey not in combine_lookup_dict:
            combine_lookup_dict[akey] = row
        else:
            # raise ValueError, 'Found duplicate row for combine_lookup_dict.  Key = ' + str(akey)
            dup_key_msg = '### Found duplicate row for combine_lookup_dict.  Key = ' + str(akey)
            print(dup_key_msg)
            log_obj.info(dup_key_msg)

    return combine_lookup_dict


def build_policy_type_lookup_dict(policy_type_rows, log_obj=None):
    ''' 
    build a lookup dictionary for policy type code and policy type name
    '''

    policy_type_lookup_dict = {}

    for row in policy_type_rows:
        # print('in build_policy_type_lookup_dict(), row: ' + str(row))
        policy_type_code = row['POLICY_TYPE_CODE']
        akey = (policy_type_code)

        if akey not in policy_type_lookup_dict:
            policy_type_lookup_dict[akey] = row
        else:
            # raise ValueError, "Got duplicate row for policy_type_lookup_dict"
            dup_key_msg = '### Found duplicate row for policy_type_lookup_dict.  Key = ' + str(akey)
            print(dup_key_msg)
            log_obj.info(dup_key_msg)

    return policy_type_lookup_dict


def get_fname(base_file_name, min_agid=0, max_agid=0, file_ext=None):
    ''' 
    build a file name using specified parameters 
    '''

    if min_agid: 
        fname_min_agid = min_agid
    else:
        fname_min_agid = 0

    if max_agid: 
        fname_max_agid = max_agid
    else:
        fname_max_agid = 'end'

    fname = str(base_file_name) + '_' + str(fname_min_agid) + '_' + str(fname_max_agid) + file_ext

    return fname


def get_dbcon(args, env, db):
    '''
    Returns a database connection object.  contained in object:
    {'server': 'XDW18VRPD01.bwcad.ad.bwc.state.oh.us', 'db': 'RPD1', 'login': 'xxxx', 'passwd': 'zzzzzz', 'type': 'vertica'}
    '''

    tgtdb = dbsetup.Envs[env][db]
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con = dblib.DB(tgtdb, log='', port = tgtdb.get('port',''))
    return con


def run_vsql( con1, sql, fname_tmp, cache = False, delim = "'\06'" ):
    '''
    IMPORTED from vertica legacy inf_vsql
    There is a bug w\python driver. So vsql is being used instead. Because data being processed is csv, the
    rows come in as strings.  Need to convert to datetime, for later date calculations.
    '''

    print( 'cache is', cache )

    if not cache or not os.path.exists( fname_tmp ):
        fname_inproc = fname_tmp+'.tmp'
        if os.path.exists( fname_tmp ): 
            print( '--- removing', fname_tmp )
            os.remove( fname_tmp )
        if os.path.exists( fname_inproc ): 
            print( '--- removing', fname_inproc )
            os.remove( fname_inproc )
        print( '-- writing to', fname_inproc )
        begin = time.time()
        print( f"SQL:  {sql}" )
        result = list( con1.fetchdict( sql ) )

        inf.write_csv( fname_inproc, result, delim = '|' )
        # result = dblib.vsql2file( db['login'], db['passwd'], fname_inproc, sql, noheader = False, gzip = True, host = db['server'], delim = delim)
        # if result != 0 : raise ValueError('vsql failure')
    
        print( '## vsql done', time.time() - begin )
        try:
            print( '## renaming', fname_inproc, fname_tmp )
        except:
            print( f'# ERR: renaming of {fname_inproc} to {fname_tmp} failed.')
            
        fsize = 0
        if os.path.exists( fname_inproc ):        
            fsize = os.path.getsize( fname_inproc )
            if fsize > 20:
                os.rename( fname_inproc, fname_tmp )
            else:
                print( 'no data???' )
                os.remove( fname_inproc )
        return len(result)
    else:
        return 0

def make_rowdict(fields,cols):
    '''
        IMPORTED from legacy vertica:  inf_vsql.py
    '''
    row_dict=dict(list(zip(fields,cols)))

    date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f' ]

    for field, val in list( row_dict.items() ):
        for df in date_formats:
            try:
                val = datetime.datetime.strptime( val, df )
                break
            except ValueError:
                pass
        else:
            val = val
    row_dict[ field ] = val            
        
    return row_dict


def get_rows( fname, delim = chr(6) ):
    '''
    IMPORTED from legacy vertica: inf_vsql
    '''
    ####Data downloaded, now build generator
    fields = []
    
    # for idx, row in enumerate( gzip.open( fname )):
    for idx, row in enumerate( open( fname,'r' )):
        row = row.strip()
        cols = row.split( delim )
        # cols = row.split( '\t' )

        str_row = str( row )
        #if '5000002' not in str_row: continue

        if idx == 0: 
            fields = [col.upper() for col in cols]
            continue
        #filter out vsql's rows comment
        if row.startswith( '(' ) and row.endswith( ' rows)' ):
            continue
        if row.startswith( '(' ) and row.endswith( ' row)' ):
            continue
        row_dict = make_rowdict( fields, cols )
        yield row_dict


def run(args, db, data_dir, prog_name, srcdb, cache, min_agid=0, max_agid=0, log_obj=None):
    ''' 
    do various tasks and generate results set
    '''

    if not min_agid: min_agid=0

    #------ Policy Type data
    policy_type_sql = get_policy_type_sql( db )
    fname_policy_type = get_fname(data_dir/prog_name, min_agid, max_agid, '_policy_type_sql.gz')
    result = run_vsql(srcdb, policy_type_sql, fname_policy_type, cache, delim='|')
    policy_type_lookup_dict = {}
    if result:
        print('creating policy type lookup dictionary using fname_policy_type: ' + fname_policy_type)
        policy_type_rows = get_rows(fname_policy_type, delim='|')
        policy_type_lookup_dict = build_policy_type_lookup_dict(policy_type_rows, log_obj=log_obj)
        # print('policy type lookup dictionary: ' + str(policy_type_lookup_dict))

    #------ Combine data
    combine_sql = get_combine_sql( db )
    fname_combine = get_fname(data_dir/prog_name, min_agid, max_agid, '_combine_sql.gz')
    result = run_vsql(srcdb, combine_sql, fname_combine, cache, delim='|')
    combine_lookup_dict = {}
    if result:
        print('creating combine lookup dictionary using fname_combine: ' + fname_combine)
        combine_rows = get_rows(fname_combine, delim='|')
        combine_lookup_dict = build_combine_lookup_dict(combine_rows, log_obj=log_obj)

    #------ Policy data
    where1 = f"WHERE P.AGRE_ID >= {min_agid} "
    # add max_agid to WHERE clause if it is specified
    if max_agid:
        where1 += f"AND P.AGRE_ID <= {max_agid} "

    sql1 = get_main_sql( db, where1)
    # fname1 = data_dir + prog_name + '_' + str(min_agid) + '_' + str(max_agid) + '_sql1.gz'
    fname1 = get_fname(data_dir/prog_name, min_agid, max_agid, '_sql1.gz')
    result = run_vsql( srcdb, sql1, fname1, cache, delim='|')

    results_dict = {}
    if not result:
        return results_dict

    log_obj.info('#### now process the rows')
    fname1 = get_fname(data_dir/prog_name, min_agid, max_agid, '_sql1.gz')
    rows1 = get_rows(fname1, delim='|')
#   results_dict = process_dbrows(rows1, combine_dict, log_obj)
    results_dict = process_dbrows(args, rows1, policy_type_lookup_dict, combine_lookup_dict, log_obj)

    return results_dict



#---------------------------------- parallel processing
def main_parallel(all_args):
    '''
       This assumes main has already created table to insert into
    '''
    args,min_agid,max_agid,PROG_NAME=all_args

    args.log=inf.setup_log(args.logdir,app=f'child_{args.table}_{min_agid}')

    #dbsetup
    tgtdb_setup = dbsetup.Envs[args.tgtenv][args.tgtkey]
    srcdb_setup = dbsetup.Envs[args.srcenv][args.srckey]

    tgt_con = dblib.DB(tgtdb_setup, log=args.log, port = tgtdb_setup.get('port',''))
    src_con = dblib.DB(srcdb_setup, log=args.log, port = srcdb_setup.get('port',''))
    if args.warehouse:
        result=list(src_con.exe(f'USE WAREHOUSE {args.warehouse}'))[0]
        args.log.info(f'{result}')

    #now start processing

    args.log.info(f'processing {min_agid=},{max_agid=}')


    table_data = run(args, args.srcdb, args.data_dir, PROG_NAME, src_con, args.cache, min_agid = min_agid, max_agid = max_agid, log_obj = args.log )
    if not table_data: 
        return f'empty table data {args.table} {min_agid}'

    args.log.info('--- Writing data file')
    # fname = data_dir + 'policy_smush_' + str(args.min_agid) + '_' + str(args.max_agid) + '.csv.gz'
    fname = get_fname(args.data_dir/'policy_smush', min_agid, max_agid, '.csv.gz')

    # inf.write_csv(fname,rows,raw=False,delim='\t',term='\n',prefix='',sortit=True,log=None,verify=False):
    inf.write_csv( fname, table_data, raw=False, delim='|', term='\n', prefix='', sortit=False)

    tgt_con.load_file( args.tgtdb,args.tgtschema, args.table, fname, delim='|', header = 1,table_part=min_agid )

    return f'finished table  {args.table} {min_agid}'

def process_results(args,results):
    for result in results:
        args.log.info(f'{result}')


#----------  Normal main
def process_args():
    '''
    Argument parser from run_example_db_many_parallel, then modified to include example command-line arguments
    '''
    parser = argparse.ArgumentParser(description='Arguments required for run_odjfs_summary_vsql',epilog='Snowflake Edition')
    #required

    parser.add_argument( '--srcdir', required = True, help='env (dev_cognos)/ key (dev_me)/ database (vertica)/ schema (DW_REPORT)' )
    parser.add_argument( '--tgtdir', required = True, help='env (dev_cognos)/ key (dev_me)/ database (vertica)/ schema (DW_REPORT)' )
    #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')

    #optional
    parser.add_argument( '--warehouse', default='', required=False, help='Snowflake Warehouse to use ')
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--cache', required = False, help = 'Cache results -- for re-runs? ')
    parser.add_argument( '--debug', default=False, action='store_true', help='Toggle for debug messaging ')
    parser.add_argument( '--dev', default=False, action='store_true', help='Changes ETL Dir from C to E if true')
    parser.add_argument( '--min_agid', required = False, default = 1000000, help = 'min_agid - e.g. 1000000' )
    parser.add_argument( '--max_agid', required = False, help = 'max_agid - e.g. 6000000' )
    parser.add_argument( '--operation', required = False, default = 'policy_hist', help = 'Operation name to show.  Default: policy_hist')
    parser.add_argument( '--table', required = False, default='DW_POLICY_INSRD_PARTICIPATION_HISTORY', help ='default table to output to: DW_POLICY_INSRD_PARTICIPATION_HISTORY' )
    parser.add_argument( '--topdir', required = False, help ='e.g.  /etl/dwdata/coxxxxbd' )
    parser.add_argument('--parallel', required=False, default=7,help='number of parallel processes')

    args= parser.parse_args()
    args.parallel = int(args.parallel)

    eldir = f"C:/temp/{os.environ['USERNAME'].replace('_x', '')}/EL/" if args.dev else f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x', '')}/EL"
    args.eldir = Path( eldir )
    
    args.root = Path(__file__).resolve().parent.parent
    args.topdir = Path( eldir )
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
    args.data_dir=args.srcdata

    return args




#--------------------------------------- main processing

def create_table(args,dbname,schema,table, con1):
    columns_str='''
    BUSN_SEQ_NO             INTEGER,
    CRNT_PLCY_PTCP_IND      TEXT,
    CUST_ID                 NUMBER,
    DW_CREATE_DTTM          DATE,
    DW_UPDATE_DTTM          DATE,
    PLCY_AGRE_ID            NUMBER,
    PLCY_NO                 TEXT,
    PLCY_PTCP_EFF_DATE      DATE,
    PLCY_PTCP_END_DATE      DATE,
    PLCY_STS_RSN_TYP_CD     TEXT,
    PLCY_STS_TYP_CD         TEXT,
    PLCY_TYP_CD             TEXT,
    PLCY_TYP_NM             TEXT,
    SUCSR_CUST_ID           NUMBER
    '''
    sql = f"""CREATE OR REPLACE  TABLE {dbname}.{schema}.{table} ({columns_str} );\n"""
    # print( sql )
    result = list( con1.exe( sql ))[0]
    if 'successfully created' not in result['status']:
        raise Warning(f' did not create table {result}')


def main():
    ''' 
    do main processing here 
    '''

    ## -- Begin added Piece
    args = process_args()    
    # Establish the top of our directory for temp files, load files, etc.
    user = os.environ['USERNAME']
    root_dir = args.topdir/user/args.srcenv/args.srcdb
    if not os.path.exists( root_dir ): os.makedirs( root_dir )


    #dbsetup
    tgtdb_setup = dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgt_con = dblib.DB(tgtdb_setup, log=args.log, port = tgtdb_setup.get('port',''))
   
    #inf2 has replaced this code, should replace later
    root_dir = args.eldir
    ## -- End added Piece

    #### OPTIONS #####
    PROG_NAME = os.path.basename(sys.argv[0]).split(".")[0]

    args.log.info('Starting run')
    create_table(args, args.tgtdb, args.tgtschema, args.table, tgt_con )

    all_args=[]
    for min_agid in range(0,3000000,100000):
        max_agid = min_agid + 99999
        all_args.append([args,min_agid,max_agid,PROG_NAME])

    all_args.append([args,3000000,20000000000,PROG_NAME])

    if len(all_args)==1 or args.parallel==1: 
        args.log.debug('Running in single threaded mode')
        results= [ main_parallel(allarg) for allarg in all_args ]
    else:
        results=inf.run_parallel(main_parallel,args=all_args,parallel=args.parallel,log=args.log)

    process_results(args,results)



    args.log.info('done')
    print('main done')


if __name__=='__main__':
    main()
    
    '''
    # -- test
    # python run_policy_hist_vsql_snow.py   --srcdir /prd/snow_etl/RPD1/PCMP  --tgtdir /uat/snow_etl/RUB1/DW_REPORT --warehouse WH_T_PROD --dev
    # -- prod
    # python run_policy_hist_vsql_snow.py   --srcdir /prd/snow_etl/RPD1/PCMP  --tgtdir prd/snow_etl/RPD1/DW_REPORT --warehouse WH_T_PROD
    
    '''
