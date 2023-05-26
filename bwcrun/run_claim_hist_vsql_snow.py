#!/usr/bin/env python
'''
USES:   PCMP.CLAIM, PCMP.PARTICIPATION, PCMP.CLAIM_PARTICIPATION, PCMP.CLAIM_PARTICIPATION_INSRD, PCMP.CLAIM_COVERAGE, PCMP.POLICY_SUMMARY_DETAIL
        BWC_RPT.CNTL_DATES_BATCH_EL
'''

'''
=============================================================================================
NOTES: 
1) set debug flag to appropriate value before executing program
2) test this program in Linux via the following commands (using sample agre_id):
   a) mkdir /etl/dwdata/coxxxbd/claim_hist_unittest (do this only if directory does not exist)
   b) rm *claim*3018244* /etl/dwdata/coxxxbd/claim_hist_unittest/*claim*3018244*
   c) python run_claim_hist_vsql.py  --operation claim_hist  --s_environment dev_cognos  --srcdb vertica  --s_schema DW_REPORT  --t_environment dev_cognos  --tgtdb vertica  --t_schema DW_REPORT  --load_type PCMP  --topdir /etl/dwdata/coxxxbd/  --min_agid 3018244  --max_agid 3018244  --cache 1
   d) cp *claim*3018244* /etl/dwdata/coxxxbd/claim_hist_unittest (necessary for executing test_run_claim_hist_vsql.py program later)
   e) zcat claim_smush_3018244_3018244.csv.gz
3) test this program in Automic via the following commands:
   a) dzdo su - svc_etl
   b) cp /etl/prod_i_drive/IT/ETL/coxxxbd/tfs/M174218_coxxxbd/DevBranch/ETL/Python/run_claim_hist_vsql.py /usr/local/etl/run
   c) chmod 755 /usr/local/etl/run/run_claim_hist_vsql.py (to prevent "Permission denied" error)
   d) execute the JOBP.DW.DW.CLAIM.INSRD.PARTICPATION.HISTORY workflow
4) after testing, execute validation queries to verify data in table
5) full test run times (in Dev)
   a) about 30 minutes (command line)
   b) about 10 minutes (Automic - with concurrent jobs)
6) per discussion with John, it is okay to use pipe as delimeter for this program
7) per discussion with Kumar, since PS does not have edits, it is possible for new situations 
   to come up in the future and they will be handled at that time
=============================================================================================
'''

# standard libraries
#std libs
import sys, argparse, os, datetime, time
from pathlib import Path

import gzip, sys, socket
# import time,os,base64,datetime,time,sys,socket,gzip
# import shutil,copy,random,pprint,operator,collections,pprint



# # local libraries
# path = '/etl/repository/hgmasters/bwclib/lib/'
# sys.path.append(path)
# from bwclib import  dblib, inf
# # from bwclib import dbsetup, dbmeta, dblib, inf, process_schema, dwargs, dbmeta_data, inf2, inf_vsql


# # set this flag to appropriate value
# debug = False
# #debug = True

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
from bwcsetup import dbsetup
from bwcenv.bwclib import inf, dblib


##### SQL START ################################################################################

def get_sql(args,where):
    '''
    The Following is the Driver query for gathering Claim participation data 
    Consolidate the Claim Participation dates when on 
        date Overlapping, effective and end date gaps,  to handle bad data at Claim participation level

        1.	Policy Combines: Check the policy if ever combined into another policy and check for complete chain of combines includes until the end of chain, these excludes PA to SI combines and SI to SI combines when BSN = 0 is not part of the predecessor policy.
        joins/ driver query;
        Join  the Policy number, Cust_id, Bsn from Policy Participation Query to Policy Combine table per below (Refereed as PS Alias). Exclude PA to SI combines and Exclude SI to SI combines when BSN = 0 is not part of the predecessor policy. If BSN-0 is part of the combine then all claims move to the successor BSN-0 policy.
        LEFT JOIN PLCY_CMBN_ALL PC 
                    ON PS.PLCY_NO = PC.PRDSR_PLCY_NO 
                    AND PS.BUSN_SEQ_NO=PC.PRDSR_BSNS_SQNC_NO 
                    AND PS.CUST_ID = PC.PRDSR_CUST_ID
            Notes;-
            a)	The Policy Combines Table has been one row Combines per Composite Key (Plcy_no, Bsn, Cust_id)
            b)	The Predecessor Policy with Same Cust_id/bsn will not have more than one combine row.
            c)	Inorder to find complete combine chain (P1 _P2 _P3 _P4). Plug the Successor Policy number back to Predecessor policy until find the complete combinations.
            d)	Resolve combine date overlaps if any (ex cust_id = 1021003). If next row combines (P2 P3) are earlier than current Combines (P1-->p2) (bad data). Per RNP the date fix would be set the date by order.
            e)	Every other combine should be in Ascending Date order (from P2 _P3 _P4).
            f)	If more than one combine happened for a day Pick the Max combine for the day (from P2 P3 P4).
            g)	If the policy has combined into another policy during the tenure of Claims insureds then the Predecessors and Successor's Cust_id will be populated based on the Claim Policy Relationship dates.

        2.	PCMP.CLAIM 
            a.	One Row Per claim Agre_id  - Primary key
            b.	To get one row per Claim Number with current details only, set the CLM_REL_SNPSHT_IND to n. 
            c.	Claim related details are stored here. Ex: Claim Occurred date, reported date, Entered date., injury details etc.
        3.	PCMP.PARTICIPATION
            a.	one row per participation type ( participation id -- primary key)
            b.	join to PCMP.CLAIM to get all the Participants on a claim., Ex: insureds, mco, emp rep, etc.
            c.	each Insureds participation id is linked to a different policy period.
            d.	Multiple customers may participate on a claim at the same time
        4.	PCMP.CLAIM_PARTICIPATION
            a.	This table stores the details of customers participating on a claim.
            b.	One row for each participation on a claim
            c.	There will be one non-voided row per participation record.
            d.	Audit fields are used for Determining the Tenure of Participation.
        5.	PCMP.CLAIM_PARTICIPATION_INSRD
            a.	This table captures the detail information for the insured participant on a claim.
            b.	It has one row per Claim Participation id.
        6.	PCMP.CLAIM_COVERAGE
            a.	This table contains the policy coverage information associated to a claim.
            b.	It has one row per claim participation insured id.
            c.	Join to Policy Summary Detail to get the Insureds Policy Number.
        7.	PCMP.POLICY_SUMMARY_DETAIL
            a.	This table contains the policy summary information for every period.
            b.	Join to Claim coverage to get policy summary information associated to a claim.
    '''

    sql = f'''
    select distinct 
           C.AGRE_ID as CLM_AGRE_ID,
           C.CLM_NO,
           P.CUST_ID as INSRD_CUST_ID,
           DATE(C.CLM_OCCR_DTM) as CLM_OCCR_DATE,
           DATE(CLM_PTCP_EFF_DT) as CLM_PTCP_EFF_DATE,
           DATE(C.AUDIT_USER_CREA_DTM) AS CLM_ENTRY_DATE,
           CP.AUDIT_USER_CREA_DTM AS SRC_PTCP_EFF_DTM,
           CP.AUDIT_USER_UPDT_DTM AS SRC_PTCP_END_DTM,
           DATE(CP.AUDIT_USER_CREA_DTM) as INSRD_PTCP_EFF_DATE,
           -- high date value is used here instead of null since that value is needed in the smush() function
           -- when comparing db_batch_row[ecol] against last_row_of_new_batch[ecol]; otherwise, will get the
           -- following error message: TypeError: can't compare datetime.datetime to str
           CASE 
               WHEN DATE(CP.AUDIT_USER_UPDT_DTM) IS NULL THEN DATE('12/31/9999') 
               ELSE DATE(CP.AUDIT_USER_UPDT_DTM) 
           END AS INSRD_PTCP_END_DATE,
           CASE 
              WHEN CP.AUDIT_USER_UPDT_DTM IS NULL THEN 'y' 
              ELSE 'n' 
           END AS CRNT_INSRD_PTCP_IND,
           CURRENT_DATE() as DW_CREATE_DTM, 
           CURRENT_DATE() as DW_UPDATE_DTM,
           PSD.PLCY_SUM_DTL_PLCY_NO AS PSD_PLCY_NO  -- added on 4/30/19 to be used in Claim Policy History merge program
      from {args.srcdb}.PCMP.CLAIM C
           inner join {args.srcdb}.PCMP.PARTICIPATION P on C.AGRE_ID = P.AGRE_ID and P.PTCP_TYP_CD = 'insrd' and C.CLM_REL_SNPSHT_IND = 'n'
           inner join {args.srcdb}.PCMP.CLAIM_PARTICIPATION CP on P.PTCP_ID = CP.PTCP_ID
           inner join {args.srcdb}.PCMP.CLAIM_PARTICIPATION_INSRD CPI on CP.CLM_PTCP_ID = CPI.CLM_PTCP_ID
           left outer join {args.srcdb}.PCMP.CLAIM_COVERAGE CC on CPI.CLM_PTCP_INSRD_ID = CC.CLM_PTCP_INSRD_ID
           left outer join {args.srcdb}.PCMP.POLICY_SUMMARY_DETAIL PSD on CC.PLCY_SUM_DTL_ID = PSD.PLCY_SUM_DTL_ID
           {where}
     order by C.AGRE_ID, C.CLM_NO, CP.AUDIT_USER_CREA_DTM, CP.AUDIT_USER_UPDT_DTM DESC, P.CUST_ID
       '''

       
    return sql

##### SQL END ##################################################################################################


def get_column_order():
    '''
    specify column order for the fields in the csv file
    '''

    col_order = ['CLM_AGRE_ID','CLM_NO','INSRD_CUST_ID','CLM_OCCR_DATE','CLM_PTCP_EFF_DATE','CLM_ENTRY_DATE',
        'SRC_PTCP_EFF_DTM','SRC_PTCP_END_DTM','INSRD_PTCP_EFF_DATE','INSRD_PTCP_END_DATE','CRNT_INSRD_PTCP_IND','DW_CREATE_DTM','DW_UPDATE_DTM','PSD_PLCY_NO']

    return col_order


def rowsdict2file(rows,fname,delim='\t',col_order=[]):
    '''
    This function produces a compressed csv file to be used by python for processing.
    The target table uses INS_PARTICIPANT instead of CUST_ID
    The column_order for the csv is determined by function get_column_order, called before
    this function.
    Initially a tmp file is written to and renamed only when completed.
    This means that if the job is terminated, the original extract is untouched.
    '''

    fname_tmp = fname + '.tmp'
    if os.path.exists(fname_tmp): os.remove(fname_tmp)
    if os.path.exists(fname): os.remove(fname)

    fw = gzip.open(fname_tmp, 'wb', compresslevel=1)
    output_rows = []
    print('writing', fname, len(rows))

    for row in rows:
        output_row = []
        for col in col_order:
            colval = row[col]

            if isinstance(colval, datetime.date) or isinstance(colval, datetime.datetime):
                # Format DATE columns as YYYY-MM-DD, else YYYY-MM-DD hh:mi:ss.xxxx
                if col.endswith('DATE'):
                    colval = colval.strftime('%Y-%m-%d')
                else:
                    colval = colval.strftime('%Y-%m-%d %H:%M:%S.%f')

            output_row.append(colval)

        final_row = delim.join(output_row)
        fw.write(final_row + '\n')
    fw.close()

    os.rename(fname_tmp, fname)
    print('finished data file', fname, time.asctime())

    return fname


def display_debugging_message(debug_msg):
    '''
    display specified debugging message
    '''

    if len(debug_msg) > 0:
        print('>>> ' + debug_msg + '\n')


def check_last_row_of_new_batch(args, new_batch, ecol):
    '''
    for the last row in new_batch list, set end_date = high_date and CRNT_INSRD_PTCP_IND = "y"
    '''

    high_date = datetime.datetime.strptime('12/31/9999', '%m/%d/%Y')

    last_row_of_new_batch = new_batch[-1]

    if last_row_of_new_batch[ecol] != high_date or last_row_of_new_batch['CRNT_INSRD_PTCP_IND'] != 'y':
        debug_msg = 'scenario 12,28,31,35,44,47,48,49 - setting end date of last row to high date and CRNT_INSRD_PTCP_IND to "y":\n' + 'last_row_of_new_batch[ecol] != high_date or last_row_of_new_batch[CRNT_INSRD_PTCP_IND] != "y"'
        if args.debug: display_debugging_message(debug_msg)

        last_row_of_new_batch[ecol] = high_date
        last_row_of_new_batch['CRNT_INSRD_PTCP_IND'] = 'y'


def merge_prev_and_curr_rows(last_row_of_new_batch, db_batch_row, db_batch, ecol, high_date):
    '''
    merge current into previous row
    '''

    last_row_of_new_batch[ecol] = db_batch_row[ecol] 
    last_row_of_new_batch['CRNT_INSRD_PTCP_IND'] = db_batch_row['CRNT_INSRD_PTCP_IND']

    # if end date of current row = high_date AND current row is the last one in the batch
    if db_batch_row[ecol] == high_date and \
        db_batch_row == db_batch[-1]:           # added this criteria for scenario #33
        last_row_of_new_batch['SRC_PTCP_EFF_DTM'] = db_batch_row['SRC_PTCP_EFF_DTM']
        last_row_of_new_batch['SRC_PTCP_END_DTM'] = db_batch_row['SRC_PTCP_END_DTM']
    else:
        # leave previous row's eff_dtm and end_dtm as is so it won't affect logic for some scenarios (i.e. 32)
        pass


def add_gap_row(args, new_batch, last_row_of_new_batch, ecol_prev_row, scol_curr_row, scol, ecol, idcol1=''):
    '''
    add gap row with appropriate values
    '''

    debug_msg = 'scenario 40,41,43 - add gap row:\n' + 'db_batch_row[scol] > last_row_of_new_batch[ecol] + aday'
    if args.debug: display_debugging_message(debug_msg)

    dummy_cust_id = '1000042034'
    aday =  datetime.timedelta(days=1)

    gap_row = dict(last_row_of_new_batch)

    gap_row[idcol1] = dummy_cust_id
    gap_row[scol] = ecol_prev_row + aday
    gap_row[ecol] = scol_curr_row - aday
    gap_row['PSD_PLCY_NO'] = ''

    new_batch.append(gap_row)


def add_dummy_row(args, new_batch, dummy_row, ccol, scol, ecol, idcol1=''):
    '''
    add dummy row with appropriate values
    '''

    debug_msg = 'scenario 7,8,10,12,18,20,21,37,40,41,45 - adding dummy row:\n' + \
        'dummy_row[ccol] < dummy_row[scol]'
    if args.debug: display_debugging_message(debug_msg)

    dummy_cust_id = '1000042034'
    aday =  datetime.timedelta(days=1)

    # e.g.  idcol1: INSRD_CUST_ID,  ecol:INSRD_PTCP_END_DATE,  scol:INSRD_PTCP_EFF_DATE, ccol:'CLM_ENTRY_DATE'
    # dummy_row['INSRD_CUST_ID'] = '1000042034'
    # dummy_row['INSRD_PTCP_END_DATE'] = '9999-12-31'
    # dummy_row['INSRD_PTCP_EFF_DATE'] = '1996-10-19'
    dummy_row[idcol1] = dummy_cust_id
    # Convert the strings back to a python Date object
    dummy_row[ecol] = datetime.datetime.strptime( dummy_row[scol], "%Y-%m-%d") - aday  # do this first before dummy_row[scol] is set to different value in next line
    dummy_row[scol] = datetime.datetime.strptime( dummy_row[ccol], "%Y-%m-%d")
    dummy_row['CRNT_INSRD_PTCP_IND'] = 'n'
    dummy_row['PSD_PLCY_NO'] = ''

    new_batch.append( dummy_row )


def process_dummy_row(args, new_batch, db_batch, ccol, scol, ecol, idcol1='', idcol2=''):
    '''
    check if a dummy row needs be created and if it needs to be smushed with the current row

    result:
    dummy row AND current row (2 rows)
                    OR
    no dummy row and current row (1 row) OR dummy row smushed into current row (1 row)
    '''

    debug_msg = ''

    dummy_row = dict(db_batch[0])   # execute this code only once per batch

    #-----------------do not add dummy row (create/claim entry date >= eff date)
    #  crea                    db_batch_row
    #  ceff     cecol          db_batch_row
    if dummy_row[ccol] >= dummy_row[scol]: 
        pass

    #-----------------add dummy row (create/claim entry date < eff date)
    #  crea                            db_batch_row
    #          ceff     cecol          db_batch_row
    else:
        add_dummy_row(args, new_batch, dummy_row, ccol, scol, ecol, idcol1)

        # process newly created dummy row along with current row in batch
        last_row_of_new_batch = new_batch[-1]
        db_batch_row = db_batch[0]

        #-----------------merge previous/dummy and current rows (same cust id)
        #  leff     lecol                        dummy_row
        #                ceff     cecol          db_batch_row
        if db_batch_row['INSRD_CUST_ID'] == last_row_of_new_batch['INSRD_CUST_ID']:
            debug_msg = 'scenario 12,37 - merge dummy row with current row (smush):\n' + 'same cust id'

            # do smushing logic below 
            del new_batch[-1]      # delete last row in the list
            db_batch_row[scol] = last_row_of_new_batch[scol]  
        else:
            debug_msg = 'scenario 7,8,10,18,20,21,40,41,45 - not merging dummy row with current row:\n' + 'diff cust id'
            pass

    if args.debug: display_debugging_message(debug_msg)


def smush(args, starting_index, db_batch, ccol, scol, ecol, idcol1='', idcol2=''):
    '''
    perform smushing logic as necessary

    If there is a gap between 2 periods, no need to add a gap row since the claims row will be used as the driver
    when joining it with policy information
    '''

    new_batch = []
    aday =  datetime.timedelta(days=1)
    high_date = datetime.datetime.strptime('12/31/9999', '%m/%d/%Y')

    process_dummy_row(args, new_batch, db_batch, ccol, scol, ecol, idcol1, idcol2)
    date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f' ]

    # add first row in current batch to new_batch list
    for field, val in list( db_batch[0].items()) :
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
            db_batch[0][ field ] = val 
    new_batch.append(db_batch[0])  


    # ******************** start of loop ********************
    # process the rest of the rows in current batch

    for db_batch_row in db_batch[starting_index:]:
        debug_msg = ''

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
        
        # print('in smush(), processing db_batch_row: \n' + str(db_batch_row))

        last_row_of_new_batch = new_batch[-1]

        last_row_of_new_batch_eff_date = last_row_of_new_batch['SRC_PTCP_EFF_DTM']
        if len(str(last_row_of_new_batch['SRC_PTCP_END_DTM'])) == 0:
            last_row_of_new_batch_end_date = datetime.datetime.strptime( '9999-12-31', '%Y-%m-%d')   # if end datetime is null then set variable to high date
        else:
            last_row_of_new_batch_end_date =  last_row_of_new_batch['SRC_PTCP_END_DTM']                        
        db_batch_row_eff_date = db_batch_row['SRC_PTCP_EFF_DTM']

        # do this step first before any other validation below
        if db_batch_row == db_batch[-1] and db_batch_row[ecol] != high_date:
            debug_msg = 'scenario 12,28,44,45 - set end date of last row in batch to high_date: \n' + 'db_batch_row == db_batch[-1]'
            # if debug: display_debugging_message(debug_msg)  # execute this statement here since control will go back to top of the loop

            db_batch_row[ecol] = high_date


        # NOTE: This rule supersedes the one below where eff date of previous and current rows are the same
        #-----------------skip current row
        #  lecol = high_date        last_row_of_new_batch
        if last_row_of_new_batch[ecol] == high_date:
            debug_msg = 'scenario 12,18,20,21,27,29,31,33,45 - skip current row: \n' + 'last_row_of_new_batch[ecol] == high_date'
            if args.debug: display_debugging_message(debug_msg)  # execute this statement here since control will go back to top of the loop

            continue    # skip current row

        #-----------------delete previous row 
        # (only if current and previous PSD policy numbers are the same; otherwise, leave the periods as is)
        #  leff            lecol              last_row_of_new_batch
        #  ceff       cecol                   db_batch_row
        elif last_row_of_new_batch_eff_date == db_batch_row_eff_date:
            debug_msg = 'scenario 5b,6,7,8,9,10,23,29,30,31,32,33,34,38,42,44,46 - delete previous row:\n' + \
                'last_row_of_new_batch_eff_date == db_batch_row_eff_date'
            # if debug: display_debugging_message(debug_msg)

            del new_batch[-1]   # delete last row in the list

            # if there are 2 or more rows in the list then reset variable to last row; otherwise, no need to do anything
            if len(new_batch) >= 1:
                last_row_of_new_batch = new_batch[-1]

                #-----------------merge previous and current rows (same cust id and same PSD policy number)
                #  leff      lecol                        last_row_of_new_batch
                #            ceff      cecol              db_batch_row
                if db_batch_row['INSRD_CUST_ID'] == last_row_of_new_batch['INSRD_CUST_ID']: 
                    debug_msg = 'scenario 5b,6,7,8,9,10,23,29,30,31,32,33,34,42,43,46,51 - merge current and previous rows (smush):\n' + \
                        'len(new_batch) >= 1 and last_row_of_new_batch_eff_date == db_batch_row_eff_date AND same cust id'
                    if args.debug: display_debugging_message(debug_msg)  # execute this statement here since control will go back to top of the loop

                    if db_batch_row['PSD_PLCY_NO'] == last_row_of_new_batch['PSD_PLCY_NO']: 
                        debug_msg = 'scenario 51 - merging current and previous rows (smush):\n' + \
                            'len(new_batch) >= 1 and last_row_of_new_batch_eff_date == db_batch_row_eff_date AND same cust id AND same PSD_PLCY_NO'
                        if args.debug: display_debugging_message(debug_msg)  # execute this statement here since control will go back to top of the loop

                        merge_prev_and_curr_rows(last_row_of_new_batch, db_batch_row, db_batch, ecol, high_date)

                        continue    # skip current row
                    else:
                        debug_msg = 'scenario 5b,6,7,8,9,10,23,29,30,31,32,33,34,42,43,46 - NOT merging current and previous rows (smush):\n' + \
                            'len(new_batch) >= 1 and last_row_of_new_batch_eff_date == db_batch_row_eff_date AND same cust id'
                        pass                    
                else:
                    pass
            else:
                pass

        #-----------------delete previous row
        #           leff                         last_row_of_new_batch
        #  lecol                                 last_row_of_new_batch
        #  ceff             cecol                db_batch_row
        # scol = 'INSRD_PTCP_EFF_DATE',   ecol: 'INSRD_PTCP_END_DATE'
        elif last_row_of_new_batch[scol] >= last_row_of_new_batch[ecol] and \
            last_row_of_new_batch[ecol] == db_batch_row[scol]: 
            debug_msg = 'scenario 30,39 - delete previous row:\n' + \
                'last_row_of_new_batch[scol] >= last_row_of_new_batch[ecol] and last_row_of_new_batch[ecol] == db_batch_row[scol]'

            del new_batch[-1]   # delete last row in the list

            # if there are 2 or more rows in the list then reset variable to last row; otherwise, no need to do anything
            if len(new_batch) >= 1:
                last_row_of_new_batch = new_batch[-1]
            else:
                pass

        #-----------------skip current row (inside date range of previous row)
        # (only if current and previous PSD policy numbers are the same; otherwise, leave the periods as is)
        #  leff         lecol             last_row_of_new_batch
        #  ceff         cecol             db_batch_row
        elif (db_batch_row[ecol] <= last_row_of_new_batch[ecol]): 
            if db_batch_row['PSD_PLCY_NO'] == last_row_of_new_batch['PSD_PLCY_NO']: 
                debug_msg = 'scenario 5b,6,9,10,23,30,32,34,36,46 - skip current row (inside date range of previous row):\n' + \
                    'db_batch_row[ecol] <= last_row_of_new_batch[ecol] AND db_batch_row[scol] >= last_row_of_new_batch[scol] AND same psd_plcy_no'
                if args.debug: display_debugging_message(debug_msg)  # execute this statement here since control will go back to top of the loop

                continue    # skip current row
            else:
                if (db_batch_row[scol] <= last_row_of_new_batch[ecol]): 
                    debug_msg = 'scenario 6,30,34,46,51 - subtracting 1 day from last_row_of_new_batch[ecol]:\n' + \
                        'db_batch_row[ecol] <= last_row_of_new_batch[ecol] AND db_batch_row[scol] <= last_row_of_new_batch[ecol] BUT diff psd_plcy_no'
                    if args.debug: display_debugging_message(debug_msg)

                    db_batch_row[scol] = last_row_of_new_batch[ecol]
                    last_row_of_new_batch[ecol] = last_row_of_new_batch[ecol] - aday
                else:
                    debug_msg = 'scenario ?? - NOT subtracting 1 day from last_row_of_new_batch[ecol]:\n' + \
                        'db_batch_row[ecol] <= last_row_of_new_batch[ecol] AND db_batch_row[scol] > last_row_of_new_batch[ecol] BUT diff psd_plcy_no'
                    if args.debug: display_debugging_message(debug_msg)

                    pass

        # db_batch_row[ecol] > last_row_of_new_batch[ecol]
        else:   
            # "db_batch_row[scol] >= last_row_of_new_batch[scol]" situation applies to code below
            # ("db_batch_row[scol] < last_row_of_new_batch[scol]" situation should not happen since data is sorted by agre_id, clm_no, eff_date)

            #-----------------delete previous row (inside date range of current row and transaction occurred on same day)
            #  leffdtm       leff      lecol                     last_row_of_new_batch
            #  ceffdtm       ceff             cecol              db_batch_row
            if last_row_of_new_batch_eff_date == db_batch_row_eff_date:
                debug_msg = 'scenario 5b,38 - delete previous row (transaction occurred on same day as current row): \n' + \
                    'last_row_of_new_batch_eff_date == db_batch_row_eff_date'

                del new_batch[-1]   # delete last row in the list

                # if there are 2 or more rows in the list then reset variable to last row; otherwise, no need to do anything
                if len(new_batch) >= 1:
                    last_row_of_new_batch = new_batch[-1]
                else:
                    pass

            #-----------------merge previous and current rows (same cust id)
            #  leff      lecol                        last_row_of_new_batch
            #            ceff      cecol              db_batch_row
            elif db_batch_row['INSRD_CUST_ID'] == last_row_of_new_batch['INSRD_CUST_ID']: 
                if db_batch_row['PSD_PLCY_NO'] == last_row_of_new_batch['PSD_PLCY_NO']: 
                    debug_msg = 'scenario 3,4,5b,6,9,32,33,34,46 - merge current and previous rows (smush):\n' + \
                        'db_batch_row[ecol] > last_row_of_new_batch[ecol] AND db_batch_row[scol] <= last_row_of_new_batch[ecol] AND same cust id AND same psd_plcy_no'
                    merge_prev_and_curr_rows(last_row_of_new_batch, db_batch_row, db_batch, ecol, high_date)

                    continue    # skip current row
                else:
                    debug_msg = 'scenario 3,4,47,48,49 - NOT merging current and previous rows (smush):\n' + \
                        'db_batch_row[ecol] > last_row_of_new_batch[ecol] AND db_batch_row[scol] <= last_row_of_new_batch[ecol] AND same cust id BUT diff psd_plcy_no'
                    pass

                if args.debug: display_debugging_message(debug_msg)  # execute this statement here since control will go back to top of the loop
                # print('last_row_of_new_batch: \n' + str(last_row_of_new_batch) + '\n' + 'db_batch_row: \n' + str(db_batch_row))

            else:
                #-----------------change eff date of current row (diff cust id)
                #  leff        lecol                    last_row_of_new_batch
                #        ceff        cecol              db_batch_row
                if db_batch_row[scol] < last_row_of_new_batch[ecol]: 
                    debug_msg = 'scenario 7,8,10,23,32,34,39,46 - change eff date of current row:\n' + \
                        'db_batch_row[scol] < last_row_of_new_batch[ecol]'

                    db_batch_row[scol] = last_row_of_new_batch[ecol] + aday

                #-----------------add gap row (diff cust id)
                #  leff        lecol+aday                                last_row_of_new_batch
                #                         ceff        cecol              db_batch_row
                elif db_batch_row[scol] > last_row_of_new_batch[ecol] + aday: 
                    add_gap_row(args, new_batch, last_row_of_new_batch, last_row_of_new_batch[ecol], db_batch_row[scol], scol, ecol, idcol1)
                else:
                    pass

        if args.debug: 
            display_debugging_message(debug_msg)

        # make sure there is no overlap between end date of previous row and eff date of current row
        if len(new_batch) >= 1 and db_batch_row[scol] == last_row_of_new_batch[ecol]: 
            if last_row_of_new_batch[scol] > db_batch_row[scol] - aday:
                debug_msg = 'scenario ?? - NOT subtracting 1 day from last_row_of_new_batch[ecol]:\n' + \
                    'len(new_batch) >= 1 and db_batch_row[scol] == last_row_of_new_batch[ecol] AND last_row_of_new_batch[scol] > db_batch_row[scol] - aday'
                if args.debug: display_debugging_message(debug_msg)

                pass
            else:
                debug_msg = 'scenario 1,2,3,4,5a,5b,6,7,8,9,10,13,15,22,25,29,30,31,32,33,34,36,37,38,39,43,44,45,46,47,48 - subtracting 1 day from last_row_of_new_batch[ecol]:\n' + \
                    'len(new_batch) >= 1 and db_batch_row[scol] == last_row_of_new_batch[ecol] AND last_row_of_new_batch[scol] <= db_batch_row[scol] - aday'
                if args.debug: display_debugging_message(debug_msg)


                if isinstance( db_batch_row[scol], datetime.datetime ):
                    last_row_of_new_batch[ecol] = db_batch_row[scol] - aday
                else:
                    last_row_of_new_batch[ecol] = datetime.datetime.strptime( db_batch_row[scol], '%Y-%m-%d') - aday

        # add row in current batch to new_batch list
        new_batch.append(db_batch_row)  

        # print('in smush(), new_batch at end of loop: ' + str(new_batch) + '\n')
    # ******************** end of loop ********************

    check_last_row_of_new_batch(args, new_batch, ecol)

    return new_batch


def process_batch(args, db_batch, ccol, scol, ecol, idcol1='', idcol2=''):
    '''
    order by C.AGRE_ID, C.CLM_NO, CP.AUDIT_USER_CREA_DTM, CP.AUDIT_USER_UPDT_DTM DESC, P.CUST_ID
    '''

    aday = datetime.timedelta(days=1)

    if len(db_batch) == 1:
        starting_index = 0
    else:
        starting_index = 1

    new_batch = smush(args, starting_index, db_batch, ccol, scol, ecol, idcol1, idcol2)

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
            raise ValueError('row must be a dict')
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
            print( 'removing', fname_tmp )
            os.remove( fname_tmp )
        if os.path.exists( fname_inproc ): 
            print( 'removing', fname_inproc )
            os.remove( fname_inproc )
        print( 'writing to', fname_inproc )
        begin = time.time()
        result = list( con1.fetchdict( sql ) )

        inf.write_csv( fname_inproc, result, delim = '|' )
        # result = dblib.vsql2file( db['login'], db['passwd'], fname_inproc, sql, noheader = False, gzip = True, host = db['server'], delim = delim)
        # if result != 0 : raise ValueError('vsql failure')

        print( 'vsql done', time.time() - begin )
        print( 'renaming', fname_inproc, fname_tmp )
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
        

    # for field, val in list(row_dict.items()):
    #     if val:
    #         val=val.strip()
    #         #2017-07-28
    #         if len(val)==10 and val.count('-')==2: # and val[0:4].isdigit() and val[5:7].isdigit() and val[9:11].isdigit():
    #             val=datetime.datetime.strptime(val,'%Y-%m-%d')
    #         #2017-07-28 01:09:59    # 2016-11-14 00:00:00
    #         elif len(val)==19 and val.count('-')==2 and val.count(':')==2: # and val[0:4].isdigit() and val[5:7].isdigit() and val[9:11].isdigit():
    #             val=datetime.datetime.strptime(val,'%Y-%m-%d %H:%M:%S')
    #         #2017-07-28 01:09:59.221874
    #         #'2017-03-14 19:04:44.707'
    #         elif len(val)>20 and '.' in val and val.count('-')==2 and val.count(':')==2: # and val[0:4].isdigit() and val[5:7].isdigit() and val[9:11].isdigit():
    #             val=datetime.datetime.strptime(val,'%Y-%m-%d %H:%M:%S.%f')
    #     row_dict[field]=val

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


def process_dbrows( args, rows, log_obj = None):
    '''
    order by C.AGRE_ID, C.CLM_NO, P.CUST_ID, INSRD_PTCP_EFF_DATE, INSRD_PTCP_END_DATE
    '''

    # for row in rows: print(sorted(row.keys()));break
    final_rows = []

    batches = get_batch( rows, 'CLM_AGRE_ID' )
    create = 'CLM_ENTRY_DATE'
    start = 'INSRD_PTCP_EFF_DATE'
    end = 'INSRD_PTCP_END_DATE'
    idcol1 = 'INSRD_CUST_ID'
    idcol2 = 'CLM_AGRE_ID'

    for rowid,agreid_batch in enumerate(batches):
        new_batch = process_batch(args, agreid_batch, create, start, end, idcol1, idcol2)
        final_rows += new_batch

    return final_rows


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


def run(args, data_dir, prog_name, con1, cache, min_agid=0, max_agid=0, log_obj=None):
    ''' 
    do various tasks and generate results set
    '''

    if not min_agid: 
        min_agid=0

    #------ Claim data
    where1 = "WHERE C.AGRE_ID >= %s " % (min_agid)

    # add max_agid to WHERE clause if it is specified
    if max_agid:
        where1 += " AND C.AGRE_ID <= %s " % (max_agid)

    sql1 = get_sql(args,where1)
    fname1 = get_fname(data_dir/prog_name, min_agid, max_agid, '_sql1.gz')
    result = run_vsql(con1, sql1, fname1, cache, delim='|')

    results_dict={}
    if result:
        args.log.info('### now process the rows')
        rows1 = get_rows(fname1, delim='|')
        results_dict = process_dbrows(args, rows1, log_obj)

    return results_dict

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


    table_data = run(args, args.data_dir, PROG_NAME, src_con, args.cache, min_agid = min_agid, max_agid = max_agid, log_obj = args.log )
    args.log.info(f'{min_agid=},{max_agid=},{len(table_data)=}')

    if not table_data:
        return f'empty table data {args.table} {min_agid}'

    args.log.info('Writing data file')
    fname = get_fname(args.data_dir/'claim_smush', min_agid, max_agid, '.csv.gz')
    
    inf.write_csv(fname, table_data, delim = '|', log=args.log)

    tgt_con.load_file( args.tgtdb, args.tgtschema, args.table, fname, delim='|', header = 1,table_part=min_agid  )

    return f'finished table  {args.table} {min_agid}'


def process_results(args,results):
    for result in results:
        args.log.info(f'{result}')


#-----------------
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
    parser.add_argument( '--cache', required=False, help='Cache results -- for re-runs? ')
    parser.add_argument( '--debug', default=False, action='store_true', help='Toggle for debug messaging ')
    parser.add_argument( '--dev', default=False, action='store_true', help='Changes ETL Dir from I to E if true')
    parser.add_argument( '--min_agid', required = False, default = 5000001,  help ='min_agid - e.g. 5000001' )
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    # parser.add_argument( '--max_agid', required = False, default = 6000000, help ='max_agid - e.g. 6000000' )
    parser.add_argument( '--max_agid', required = False, default = 5001000, help ='max_agid - e.g. 6000000' )
    parser.add_argument( '--operation', required = False, default='claim_hist', help = 'Operation to run.  Default: odjfs_summary')
    parser.add_argument( '--table', required = False, default = 'DW_CLAIM_INSRD_PARTICIPATION_HISTORY', help ='table to load into' )
    parser.add_argument( '--topdir', required = False, help ='e.g.  /etl/dwdata/coxxxxbd' )
    parser.add_argument('--parallel', required=False, default=7,help='number of parallel processes')



    args= parser.parse_args()
    args.parallel = int(args.parallel)

    eldir = f"C:/temp/{os.environ['USERNAME'].replace('_x', '')}/EL/" if args.dev else f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x', '')}/EL"
    args.eldir = eldir
    
    args.root = Path(__file__).resolve().parent.parent
    args.topdir = eldir
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
    
def create_table(args,dbname,schema,table, con1):
    columns_str='''
        CLM_AGRE_ID             numeric(31,0),
        CLM_ENTRY_DATE          date,
        CLM_NO                  TEXT,
        CLM_OCCR_DATE           date,
        CLM_PTCP_EFF_DATE       date,
        CRNT_INSRD_PTCP_IND     TEXT,
        DW_CREATE_DTM           timestamp,
        DW_UPDATE_DTM           timestamp,
        INSRD_CUST_ID           numeric(31,0),
        INSRD_PTCP_EFF_DATE     date,
        INSRD_PTCP_END_DATE     date,
        PSD_PLCY_NO             TEXT,
        SRC_PTCP_EFF_DTM        timestamp,
        SRC_PTCP_END_DTM        timestamp
    '''
    sql = f"""CREATE OR REPLACE  TABLE {dbname}.{schema}.{table} ({columns_str} );\n"""
    print( sql )
    result = list( con1.exe( sql ))[0]
    if 'successfully created' not in result['status']:
        raise Warning(f' did not create table {result}')


def main():
    ''' 
    do main processing here 
    '''

    ## -- Begin added Piece
    args = process_args()    
    host = socket.gethostname().split('.')[0]
    # Establish the top of our directory for temp files, load files, etc.

    # user = os.environ['USERNAME']
    # /etl/dwdata/dw_el/svc_etl/xdw15vrub01
    # topdir = '/etl/dwdata', 'dw_el', user= 'svc_etl'. host= 'xdw15vrub01'
    root_dir = os.path.join( args.topdir,'dw_el', host )

    if not os.path.exists( root_dir ): os.makedirs( root_dir )
    tgtdb_setup = dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgt_con = dblib.DB(tgtdb_setup, log=args.log, port = tgtdb_setup.get('port',''))

    root_dir = args.eldir

    ## -- End added Piece

    #### OPTIONS #####
    PROG_NAME = os.path.basename(sys.argv[0])

    ###CREATE TARGET DATA:run job to create table data
    args.log.info('Starting run')
    create_table(args, args.tgtdb, args.tgtschema, args.table, tgt_con )

    all_args = []

    for min_agid in range(0,30000000,1000000):
        max_agid = min_agid + 999999
        all_args.append([args,min_agid,max_agid,PROG_NAME])
    all_args.append([args,30000000,20000000000,PROG_NAME])
  
    # for i in all_args:
    #     print(i)
    # input('go')


    #args.parallel=1
    if len(all_args)==1 or args.parallel==1: 
        args.log.debug('Running in single threaded mode')
        results=[]
        for allarg in all_args:
            result=main_parallel(allarg)
            results.append(result)

        #results= [ main_parallel(allarg) for allarg in all_args ]
    else:
        results=inf.run_parallel(main_parallel,args=all_args,parallel=int(args.parallel),log=args.log)

    process_results(args,results)

    args.log.info('done')
    print('main done')


if __name__=='__main__':
    main()
    
    '''
    -dev
    python run_claim_hist_vsql_snow.py   --srcdir /prd/snow_etl/RPD1/PCMP  --tgtdir /uat/snow_etl/RUB1/DW_REPORT --dev

    -prd
    #python run_claim_hist_vsql_snow.py   --srcdir /prd/snow_etl/RPD1/PCMP  --tgtdir /prd/snow_etl/RPD1/DW_REPORT
    
    '''
