#!/usr/bin/env python
'''
CBC 2022-09-29: Addition of the build of the step1 table {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP
                Initial conversion from Vertica to Snowflake and Python 3.X
'''
import argparse, datetime, gzip, os, sys, socket, time, collections, csv, copy, operator
from genericpath import exists
from email.utils import collapse_rfc2231_value
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

#std libraries
#import time,os,base64,datetime,time,sys,socket,gzip
#import shutil,copy,random,pprint,operator,collections


#local libraries
#path='/etl/repository/hgmasters/bwclib/lib/'
#sys.path.append(path)
#from bwclib import dbsetup, dbmeta, dblib, inf, process_schema, dwargs, dbmeta_data, inf2


def write_datafile(resultset,data_dir,senario='',delim='\t',col_order=[]):
    '''
    This function produces a compressed csv file to be used by python for processing.
    The target table uses INS_PARTICIPANT instead of CUST_ID
    The column_order for the csv is determined by function get_column_order, called before
    this function.
    Initially a tmp file is written to and renamed only when completed.
    This means that if the job is terminated, the original extract is untouched.
    
    '''
    if senario:
        fname=data_dir+'CLAIM_POLICY_HISTORY_file'+str(senario)+'_CLAIM_POLICY_HISTORY.csv'
    else:
        fname=data_dir+'table_data_CLAIM_POLICY_HISTORY.csv'

    fname_tmp=fname+'.tmp'
    if os.path.exists(fname_tmp): os.remove(fname_tmp)
    if os.path.exists(fname): os.remove(fname)

    fw=gzip.open(fname_tmp,'wb',compresslevel=1)
#    fw.write('\t'.join(col_order)+'\n')
    output_rows=[]
    print('writing',fname,len(resultset))
    oldrow=[]
    end_day=datetime.datetime(9999,12,31,0,0)
    now=datetime.datetime.now()
 
    for row in rows: 
        output_row=[]
        for col in col_order:
            try:
                if col =='INS_PARTICIPANT':
                    col='CUST_ID'
                output_row.append(str(getattr(row,col)))
            except:
                print('missing',col)
            final_row=delim.join(output_row)
            fw.write(final_row+'\n')
    fw.close()
    
    os.rename(fname_tmp,fname)
    print('finished data file',fname,time.asctime())

    return fname

def get_column_order():
    '''
    This function is used to ensure the same column order is used when processing the data.
    This is necessary when processing csv files.
    '''
    col_order=['CLM_AGRE_ID','CLM_NO','INS_PARTICIPANT','CLM_OCCR_DATE',
          'CLM_OCCR_DTM','CLM_PTCP_EFF_DATE','CLM_PTCP_EFF_DT','CLM_PLCY_RLTNS_EFF_DATE',
          'CLM_PLCY_RLTNS_EFF_DT','CLM_PLCY_RLTNS_END_DATE','CLM_PLCY_RLTNS_END_DT','PLCY_NO',
          'PLCY_AGRE_ID','BUSN_SEQ_NO','CTL_ELEM_SUB_TYP_CD','CTL_ELEM_SUB_TYP_NM','CRNT_PLCY_IND',
          'CTL_ELEM_TYP_CD','DW_CREATE_DTTM','DW_UPDATE_DTTM',]

    return col_order


def writecsv( args, fname_inproc, results ):
    print( '\n--- Writing CSV ---')

    # unpack the list of lists for write_csv from infrastructure to use
    csv_list = []
    for rw in results[0]:
        csv_list.append( rw )

    now = datetime.now()
    datestring = now.strftime("%Y%m%d")

    fields_avail = csv_list[0]

    comparison_file = fname_inproc

    # Correct the SQL columns (DB1_SQL, DB2_SQL) to change ' to '' for inserting into SF
    args.fields = list( csv_list[0].keys() )
    print(f'**** FIELDS: {args.fields}')
    # write_csv(fname,rows,raw=False,delim='\t',term='\n',prefix='',sortit=True,log=None,verify=False):
    with open( args.etldir/comparison_file, 'w', newline = '') as file2write:
        with file2write:
            csvoutput = csv.DictWriter( file2write, args.fields )
            csvoutput.writeheader()
            csvoutput.writerows( csv_list )

    # inf.write_csv( args.etldir/comparison_file, csv_list )
    print( f' --- Wrote CSV file to: {comparison_file}\n')

    return comparison_file


def run_vsql( args, db, sql, fname_tmp ):
    '''
    There is a bug w\python driver. So vsql is being used instead.

    Because data being processed is csv, the rows come in as strings.
    You may need to convert to datetime, for later date calculations.
    '''

    #print( sql )
    #print(fname_tmp)
    db1_conn = get_dbcon( args, args.srcenv, args.srckey)

    if not args.cache or not os.path.exists( fname_tmp ):
        #print(f'VSQL - {sql}')
        fname_inproc = fname_tmp +'.tmp'
        if os.path.exists( fname_tmp ): 
            print( 'removing', fname_tmp )
            os.remove( fname_tmp )
        if os.path.exists( fname_inproc ): 
            print( 'removing', fname_inproc )
            os.remove( fname_inproc )
        print( 'writing to', fname_inproc )
        begin = time.time()

        results =  db1_conn.fetchall( sql ) 
        #print(f'results={results}')
        #query_output = writecsv( args, fname_inproc, results )
        #writecsv( args, fname_inproc, results )
        inf.write_csv( fname_inproc, results, delim='\t' )
        print(f'write complete {fname_inproc}')
        #results = dblib.vsql2file(db['login'], db['passwd'], fname_inproc, sql, noheader=False, gzip=True, host=db['server'], delim="'\06'")

        #if results !=0 : raise ValueError( 'sql failure' )

        print( 'sql done', time.time()-begin )
        print( 'renaming', fname_inproc, fname_tmp )
        fsize = 0
        if os.path.exists( fname_inproc ):        
            fsize=os.path.getsize( fname_inproc )
            if fsize > 20:
                os.rename( fname_inproc, fname_tmp )
            else:
                print ( 'no data???' )
                os.remove( fname_inproc )
    else:
        print( 'using args.cache', fname_tmp )


def get_dbcon( args, env, db ):
    '''
    Returns a database connection object.  contained in object:
    {'server': 'XDW18VRPD01.bwcad.ad.bwc.state.oh.us', 'db': 'RPD1', 'login': 'xxxx', 'passwd': 'zzzzzz', 'type': 'vertica'}
    '''

    tgtdb = dbsetup.Envs[env][db]
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con = dblib.DB( tgtdb, log=args.log, port = tgtdb.get('port','') )
    return con


def convert_string_cols2datetime( cols ):
    '''
    This function converts the csv data which are dates from string into datetime

    '''

    new_cols = []
    for val in cols:
        if val:
            val = val.strip()
            #2017-07-28
            if len( val ) == 10 and val.count( '-' ) == 2: # and val[0:4].isdigit() and val[5:7].isdigit() and val[9:11].isdigit():
                val = datetime.datetime.strptime( val, '%Y-%m-%d' )
            #2017-07-28 01:09:59    # 2016-11-14 00:00:00
            elif len( val ) == 19 and val.count( '-' ) == 2 and val.count( ':' ) == 2: # and val[0:4].isdigit() and val[5:7].isdigit() and val[9:11].isdigit():
                val = datetime.datetime.strptime( val, '%Y-%m-%d %H:%M:%S' )
            #2017-07-28 01:09:59.221874   #'2017-03-14 19:04:44.707'
            elif len( val ) > 20 and '.' in val and val.count( '-' ) == 2 and val.count( ':' ) == 2: # and val[0:4].isdigit() and val[5:7].isdigit() and val[9:11].isdigit():
                val = datetime.datetime.strptime( val, '%Y-%m-%d %H:%M:%S.%f' )            

        new_cols.append( val )
    
    return new_cols

def get_rows( fname_tmp, delim = chr(6) ):
    '''

    This is a generator that returns 1 row at a time, in order to not use up memory

    Processes compressed csv file.
        by default the delimeter is 'binary' 6 so we don't have to worry about tabs or commas
        first row is the header and save as upper case in col_names
        The header also gives the format of the named tuple instead of regular tuple 0,1,2,3
            i.e. location 0 is FIRST_NAME, location 1 is LAST_NAME

    
    john to give comment
    open the gzip file , strip, split, collections? why are we checking positions. what is the filter we are checking?
    what is this ntuple_of_columns?

    namedtuple allow for:
        row.firstname instead of a dicts: row['firstname']
        it is also more memory efficient

    convert_strings converts csv strings into actual datatypes like datetime        
    '''
    print(f'Opening {fname_tmp}')
    if os.path.exists( fname_tmp ):
        fields = []

        date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f' ]
        
        input_file = inf.read_csv( fname_tmp, delim='\t' )

        for row in input_file:

            for field, val in list( row.items() ):
                if '_DATE' in field or '_DTM' in field or '_DT' in field:
                    if not isinstance( val, datetime.datetime ):
                        for df in date_formats:
                            try:
                                val = datetime.datetime.strptime( val, df )
                                break
                            except ValueError:
                                pass
                        else:
                            val = val
                    row[ field ] = val    

            yield row
            #print(row)

        # for idx, row in enumerate( open( fname_tmp ) ):

        #     row = row.strip()
        #     print(f'row = {row}')
        #     #cols = row.split( delim ) #convert string into a list
        #     cols = row.split( '\t' ) #convert string into a list
        #     print(f'cols = {cols}')

        #     if idx == 0:
        #         col_names = [ col.upper() for col in cols ]
        #         # now we have col names: CRNT_PLCY_IND,CTL_ELEM_SUB_TYP_CD
        #         named_tuple_template = collections.namedtuple( 'row', col_names )
        #         continue

        #     values = cols
        #     #filter out vsql's rows comment
        #     if row.startswith( '(' ) and row.endswith( ' rows)' ):
        #         continue
        #     if row.startswith( '(' ) and row.endswith( ' row)' ):
        #         continue
        #     #now we have col names and values 
        #     #  CTL_ELEM_SUB_TYP_CD='pa', CRNT_PLCY_IND='n'
        #     values = convert_string_cols2datetime( values )  #now we have actual dataypes
        #     row = named_tuple_template( *values )  #provide values for each location

#################################################################################3

def get_sql( args, where = '' ):
    '''
    driver sql - this is selecting minimum effective date from the batch set of
    agreid else the claim_audit_Create as the claim policy effective date.

    Per AGRE_ID:
        CLM_PLCY_RLTNS_EFF_DT:
            if CLM_PLCY_RLTNS_EFF_DT is the min then use PCMP.CLAIM.AUDIT_USER_CREA_DTM
            A window function is used to get the min CLM_PLCY_RLTNS_EFF_DT for the partition of CLM_AGRE_ID
        CLM_REL_SNPSHT_IND='n':
             why?
        DW_CLAIM_POLICY_HISTORY_TEMP:
             what created it?

    

    '''
    sql = f'''
    SELECT A.CLM_AGRE_ID,A.CLM_NO,
    
    (case  
    when date_trunc('day',CLM_PLCY_RLTNS_EFF_DT) = date_trunc('day',MIN(CLM_PLCY_RLTNS_EFF_DT) over (partition by CLM_AGRE_ID order by CLM_PLCY_RLTNS_EFF_DT, CLM_PLCY_RLTNS_END_DT))
    then date_trunc('day',B.AUDIT_USER_CREA_DTM) 
    else date_trunc('day',CLM_PLCY_RLTNS_EFF_DT)   END )as CLM_PLCY_RLTNS_EFF_DT,
    
    coalesce(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')) as CLM_PLCY_RLTNS_END_DT,
    A.CTL_ELEM_SUB_TYP_CD,A.CRNT_PLCY_IND

    FROM {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A
    INNER JOIN {args.tgtdb}.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    {where}
    and B.CLM_REL_SNPSHT_IND = 'n' 
    ORDER BY CLM_AGRE_ID,CLM_PLCY_RLTNS_EFF_DT,CLM_PLCY_RLTNS_END_DT
    '''

    return sql

    
def get_sql2( args, where = '' ):
    '''
      get the PEC and PES rows for CPRH.PRFL_STMT_ID = '6000108' AND CPRH.VOID_IND = 'n'

    DW_CLAIM_POLICY_HISTORY_TEMP
        driver
        used to filter: for pec,pes (CTL_ELEM_SUB_TYP_CD)
        
    PCMP.CLAIM_PROFILE_HISTORY:
        
    BWCODS.TDDMINC:
        SOC_MNL_CLS_TYPE = 'P'

    CLM_REL_SNPSHT_IND='n':
             why?
 
    '''
    
    sql = f'''
    SELECT CPRH.AGRE_ID as CLM_AGRE_ID ,CPRH.CLM_PRFL_ANSW_TEXT,'P' AS SOC_MNL_CLS_TYPE,
    date_trunc('day',CPRH.HIST_EFF_DTM) as HIST_EFF_DTM,
    coalesce(date_trunc('day',CPRH.HIST_END_DTM),TO_DATE('9999-12-31','YYYY-MM-DD')) as HIST_END_DTM,
    coalesce(MINO.MOD_INDST_CODE,'11') as MOD_INDST_CODE ,
    coalesce(date_trunc('day',MINO_BGN_DATE),TO_DATE('1900-01-01','YYYY-MM-DD')) as MINO_BGN_DATE,
    coalesce(date_trunc('day',MINO_END_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'))as MINO_END_DATE,
    coalesce(MINC.MOD_INDST_DESC,'UNKNOWN') as MOD_INDST_DESC ,A.CLM_NO
    FROM 
     {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A 
     INNER JOIN {args.tgtdb}.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    left outer join {args.tgtdb}.PCMP.CLAIM_PROFILE_HISTORY CPRH ON CPRH.AGRE_ID = A.CLM_AGRE_ID
      LEFT OUTER JOIN {args.tgtdb}.BWCODS.TDDMINO MINO
    ON CPRH.CLM_PRFL_ANSW_TEXT = MINO.SOC_MNL_CLS_NO AND MINO.SOC_MNL_CLS_TYPE = 'P' 
    left outer join {args.tgtdb}.BWCODS.TDDMINC MINC
    ON MINO.MOD_INDST_CODE = MINC.MOD_INDST_CODE
    WHERE CPRH.PRFL_STMT_ID = '6000108' AND CPRH.VOID_IND = 'n'
    AND A.CTL_ELEM_SUB_TYP_CD IN  ('pec','pes')
    and B.CLM_REL_SNPSHT_IND = 'n' 
    AND ((CPRH.HIST_EFF_DTM BETWEEN date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')))
                 OR (date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT) BETWEEN date_trunc('day',CPRH.HIST_EFF_DTM) AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')))) 
    
    {where}
    ORDER BY CPRH.AGRE_ID,CPRH.HIST_EFF_DTM,MINO_BGN_DATE,MINO_END_DATE


    '''

    return sql

def get_sql3(args, where = '' ):
    
    '''
    get 'pa' rows and B.CLM_REL_SNPSHT_IND = 'n'
    '''
    
    sql=f'''
    SELECT CCCH.CLM_AGRE_ID ,CCCH.ADJST_CLASS_CODE,
    CASE WHEN date_trunc('day',B.CLM_OCCR_DTM) < '1996-07-01' THEN 'S' ELSE 'N' END AS SOC_MNL_CLS_TYPE,
    date_trunc('day',CCCH.CLASS_CD_EFF_DT) as HIST_EFF_DTM,
    coalesce(date_trunc('day',CCCH.CLASS_CD_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')) as HIST_END_DTM,
    coalesce(MINO.MOD_INDST_CODE,'11') as MOD_INDST_CODE ,
    coalesce(date_trunc('day',MINO_BGN_DATE),TO_DATE('1900-01-01','YYYY-MM-DD')) as MINO_BGN_DATE,
    coalesce(date_trunc('day',MINO_END_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'))as MINO_END_DATE,
    coalesce(MINC.MOD_INDST_DESC,'UNKNOWN') as MOD_INDST_DESC ,
    A.CLM_NO
    FROM 
     {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A 
     INNER JOIN {args.tgtdb}.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    left outer join {args.tgtdb}.DW_REPORT.DW_CLAIM_CLASS_CODE_HISTORY CCCH ON CCCH.CLM_AGRE_ID = A.CLM_AGRE_ID
    LEFT OUTER JOIN {args.tgtdb}.BWCODS.TDDMINO MINO
    ON CCCH.ADJST_CLASS_CODE = MINO.SOC_MNL_CLS_NO AND MINO.SOC_MNL_CLS_TYPE =(CASE WHEN date_trunc('day',B.CLM_OCCR_DTM) < '1996-07-01' THEN 'S' ELSE 'N' END )
    left outer join {args.tgtdb}.BWCODS.TDDMINC MINC
    ON MINO.MOD_INDST_CODE = MINC.MOD_INDST_CODE
    WHERE B.CLM_REL_SNPSHT_IND = 'n'
    AND A.CTL_ELEM_SUB_TYP_CD IN  ('pa')
        {where}
    AND ((CCCH.CLASS_CD_EFF_DT BETWEEN date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')))
          OR (A.CLM_PLCY_RLTNS_EFF_DT BETWEEN date_trunc('day',CCCH.CLASS_CD_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')))) 
    ORDER BY CCCH.CLM_AGRE_ID,CCCH.CLASS_CD_EFF_DT,MINO_BGN_DATE,MINO_END_DATE


    
    '''

    return sql


def get_sql4(args, where = '' ):
    '''
    get rows not in pec,pes,pa  (like 'si' etc.,)
    '''
    sql=f'''
    SELECT CCCH.CLM_AGRE_ID,CCCH.ADJST_CLASS_CODE,'S' AS SOC_MNL_CLS_TYPE ,
    date_trunc('day',CCCH.CLASS_CD_EFF_DT) as HIST_EFF_DTM,
    coalesce(date_trunc('day',CCCH.CLASS_CD_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')) as HIST_END_DTM, 
    coalesce(MINO.MOD_INDST_CODE,'11') as MOD_INDST_CODE,
    coalesce(date_trunc('day',MINO_BGN_DATE),TO_DATE('1900-01-01','YYYY-MM-DD')) as MINO_BGN_DATE,
    coalesce(date_trunc('day',MINO_END_DATE),TO_DATE('9999-12-31','YYYY-MM-DD'))as MINO_END_DATE,
    coalesce(MINC.MOD_INDST_DESC,'UNKNOWN') as MOD_INDST_DESC,
    A.CLM_NO
    FROM 
     {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP A 
     INNER JOIN {args.tgtdb}.PCMP.CLAIM B ON A.CLM_NO = B.CLM_NO
    left outer join {args.tgtdb}.DW_REPORT.DW_CLAIM_CLASS_CODE_HISTORY CCCH ON CCCH.CLM_AGRE_ID = A.CLM_AGRE_ID
    LEFT OUTER JOIN {args.tgtdb}.BWCODS.TDDMINO MINO
    ON CCCH.ADJST_CLASS_CODE = MINO.SOC_MNL_CLS_NO AND MINO.SOC_MNL_CLS_TYPE = 'S'
    left outer join {args.tgtdb}.BWCODS.TDDMINC MINC
    ON MINO.MOD_INDST_CODE = MINC.MOD_INDST_CODE
    WHERE B.CLM_REL_SNPSHT_IND = 'n'
    AND (A.CTL_ELEM_SUB_TYP_CD not IN  ('pec','pes','pa') or A.CTL_ELEM_SUB_TYP_CD is null)
    AND ((CCCH.CLASS_CD_EFF_DT BETWEEN date_trunc('day',A.CLM_PLCY_RLTNS_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')))
          OR (A.CLM_PLCY_RLTNS_EFF_DT BETWEEN date_trunc('day',CCCH.CLASS_CD_EFF_DT)  AND NVL(date_trunc('day',A.CLM_PLCY_RLTNS_END_DT),TO_DATE('9999-12-31','YYYY-MM-DD')))) 
    {where}

    ORDER BY CLM_AGRE_ID,CCCH.CLASS_CD_EFF_DT,MINO_BGN_DATE,MINO_END_DATE



    '''

    return sql

def remove_duplicates( row_batches ):
    '''
    This is a generator (to save memory) which removes duplicate
    rows from a batch (ex: bunch of rows for one agre_id)
    
    batches:
        processes groups of rows at a time
        whenever current row is same as old, skip it
        yield new batch when whole group is processed
        
    '''
    for row_batch in row_batches:
        #raw_input('go remove')
        new_rows = []
        for row in row_batch:
            if not new_rows:
                new_rows.append( row ) 
                continue
            if new_rows[ -1 ] == row:
                continue
            new_rows.append( row )
        #print 'remove_dup',len(row_batch),len(new_rows)
        #pprint.pprint(new_rows)
        yield new_rows

def smush_row2( old_rows, cur_row, eff, end ):
    '''
    This uses rows that are batched on agre_id and smush type(pec,pes,etc)

    A named tuple is used here
        for pattern based: getattr is used to get columns specified by eff,end

    senario #1
    same: return old_rows 
    last:   eff ---- end
    curr:   eff ---- end
    [drop cur_row]

    senario #2     
    last:   eff ---- end [aday or same]
    curr:                eff ---- end
    last:   eff ----------------- end
    [drop cur_row]

    senario #3     
    else [append cur_row]

    import collections
    ntuple=collections.namedtuple('row',['a','b'])
    ntuple #<class '__main__.row'>
    row=ntuple(1,2)
    row  #row(a=1, b=2)
    getattr(row,'a') now same as row[0] for normal tuple

    '''

    aday =  datetime.timedelta( days = 1 )
    end_day = datetime.datetime( 9999, 12, 31, 0, 0 )
    last_row = old_rows[ -1 ]

    last_end = last_row[end]
    cur_end = cur_row[end]

    if last_end == '' :
        last_row[end] = end_day
    if cur_end == '' :
        cur_row[end] = end_day

    ##DATA transformations
    #ignore new row if end dates match, begin is fine, comes in sorted, removes duplicates on the eff & end dates
    if last_row[end] == cur_row[end]:
        return old_rows
    
    ##only smush if the date is continous
    if last_row[end] + aday == cur_row[eff] or last_row[end] == cur_row[eff] :
        last_row[end] = cur_row[end]
        old_rows[-1]=last_row
  
        return old_rows
    else:
        ##NO DATA transformations needed
        old_rows.append( cur_row )
        return old_rows

def smush_row2_dict( rows, eff, end, indst_code, template_row, now):
    '''
        This function assumes that all of the rows in the batch are ok to smush together
        when eff date of the curr row is close to or matchs the end date of the last row
        This logic is show below
        BEFORE
        #  last_eff     last_end [aday]
        #                             cur_eff  cur_end
        AFTER
        #  last_eff    [last_end]------------->last_end [will be cur_end]
        #   DROPPED                   cur_eff  cur_end
    '''

    aday =  datetime.timedelta(days=1)
    end_day = datetime.datetime(9999, 12, 31, 0, 0)

    
    final_rows = []
    new_row = []
    old_row = []
    for cur_row in rows:
        
        #1st time always append
       
        ##change done 2/2/2018
        if cur_row['INDST_CD_END_DT'] < cur_row['INDST_CD_EFF_DT']:
            continue

        if not final_rows:
            final_rows.append(cur_row)
            continue
        
        last_row = final_rows[-1]
        
        
        last_end = last_row[end]
        cur_end = cur_row[end]

        if last_end == '' :
            last_row[end] = end_day
        if cur_end== '' :
            cur_row[end] = end_day

        #drop the current row
        if last_row[end] == cur_row[end]:
            continue
        
        #  last_eff last_end [aday]
        #                    cur_eff  cur_end
        #  last_eff                   last_end   move last day out
        # continous check: move end date out, drop the current row
        if ( last_row[end] != end_day and ( last_row[end] + aday == cur_row[eff] ) and last_row[indst_code] == cur_row[indst_code]) or (last_row[end] ==cur_row[eff] and last_row[indst_code] == cur_row[indst_code]) :
            last_row[end] = cur_row[end]
            final_rows[-1] = last_row #do we need???
            continue
        # within check: move end date out, drop the current row
        if last_row[end] <= cur_row[end] and last_row[eff] <= cur_row[eff] :
            last_row[end] = cur_row[end]
            final_rows[-1] = last_row 
            continue

        final_rows.append(cur_row)

    return final_rows

###the main code3 which does lot of logic on the rows and the smushing and writes error/email on the log if there are rows not in the correct order

def smush_row_indst( rows, eff, end, indst_code, template_row, now ):
    '''

    The dates cannot be smushed independant of INDST_CD. This function sends a batch of rows with the same
    INDST_CD to smush_row2_dict to collapse rows togther if the dates line up. This is show below where
    INDST_CD 8 is reduced to one row with the end dt pushed out.

    Smush based off of:INDST_CD,   INDST_CD_EFF_DT,  INDST_CD_END_DT 
BEFORE
7052977 13-345378       11      UNKNOWN 2013-09-17 00:00:00     2013-09-17 00:00:00     n       2018-01-29 14:00:19.503149      2018-01-29 14:00:19.503149
7052977 13-345378       8       SERVICE 2013-09-18 00:00:00     2013-09-26 00:00:00     n       2018-01-29 14:00:19.503149      2018-01-29 14:00:19.503149
7052977 13-345378       8       SERVICE 2013-09-27 00:00:00     2013-10-08 00:00:00     n       2018-01-29 14:00:19.503149      2018-01-29 14:00:19.503149
7052977 13-345378       11      UNKNOWN 2013-10-09 00:00:00     2016-11-10 00:00:00     n       2018-01-29 14:00:19.503149      2018-01-29 14:00:19.503149
7052977 13-345378       11      UNKNOWN 2016-11-11 00:00:00                             y       2018-01-29 14:00:19.503149      2018-01-29 14:00:19.503149
AFTER
7052977 13-345378       11      UNKNOWN 2013-09-17 00:00:00     2013-09-17 00:00:00     n       2018-01-30 12:22:07.827482      2018-01-30 12:22:07.827482
7052977 13-345378       8       SERVICE 2013-09-18 00:00:00     2013-10-08 00:00:00     n       2018-01-30 12:22:07.827482      2018-01-30 12:22:07.827482
7052977 13-345378       11      UNKNOWN 2013-10-09 00:00:00                             y       2018-01-30 12:22:07.827482      2018-01-30 12:22:07.827482


    '''

    aday =  datetime.timedelta(days=1)
    end_day = datetime.datetime(9999, 12, 31, 0, 0)

    rows.sort ( key = operator.itemgetter( 'CLM_AGRE_ID', 'INDST_CD_EFF_DT', 'INDST_CD_END_DT' ) )
    last_row = rows[-1]

    if last_row['INDST_CD_END_DT'] == end_day:
        pass
    else:
        #create dummy row of 12/31/9999 if incoming batch doesn't have 12/31/9999
        new_row = copy.deepcopy( template_row )
    
        new_row['INDST_CD']='11'
        new_row['INDST_NM']='UNKNOWN'
        new_row['INDST_CD_EFF_DT']=last_row['INDST_CD_END_DT'] + aday
        new_row['INDST_CD_END_DT']=end_day

        new_row['CRNT_IND']=''
        new_row['DW_CREATE_DTTM']=now
        new_row['DW_UPDATE_DTTM']=now
        new_row['CLM_AGRE_ID']= last_row['CLM_AGRE_ID']
        new_row['CLM_NO']= last_row['CLM_NO']
        
        rows.append(new_row)

    output_rows=[]    
    smushed_rows=[]
    cur_indst=''
    ##change 2/2/2018
    
    rows.sort(key=operator.itemgetter('CLM_AGRE_ID','INDST_CD_EFF_DT','INDST_CD_END_DT')) 
    #smush dates in previous batch whenver industry type code changes
    for row in rows:
        if cur_indst != row['INDST_CD'] and smushed_rows:
            output_rows+=smush_row2_dict(smushed_rows,eff,end,indst_code,template_row,now)
            smushed_rows=[]
        smushed_rows.append(row)
        cur_indst=row['INDST_CD']

    #smush last batch catch the final industry change(which is all the same ind code)
    if smushed_rows:
        smushed_rows=smush_row2_dict(smushed_rows,eff,end,indst_code,template_row,now)
        output_rows+=smushed_rows

    ##change 3/14/2018
    ##This is to fillup the rows with "11" testcase # '12-838891','10-828913','13-345378'
    #fill in gaps as type 11 for all gaps in batch
    output_rows.sort(key=operator.itemgetter('CLM_AGRE_ID','INDST_CD_EFF_DT','INDST_CD_END_DT'))
    
    new_row=[]
    old_row=[]
    #print 'output_rows'
    #pprint.pprint(output_rows)
    output_rows_new=list(output_rows)
    for row in output_rows:
        if not old_row:
            old_row=row
            continue
        #print row['CLM_AGRE_ID']
        
        if old_row['INDST_CD_END_DT']+aday !=row['INDST_CD_EFF_DT'] and old_row['INDST_CD_END_DT'] != row['INDST_CD_END_DT']:

            new_row=copy.deepcopy(template_row)
            new_row['INDST_CD_EFF_DT']=old_row['INDST_CD_END_DT']+aday
            new_row['INDST_CD_END_DT']=row['INDST_CD_EFF_DT']- aday
            new_row['INDST_CD']='11'
            new_row['INDST_NM']='UNKNOWN'
            new_row['DW_CREATE_DTTM']=now
            new_row['DW_UPDATE_DTTM']=now
            new_row['CLM_AGRE_ID']= row['CLM_AGRE_ID']
            new_row['CLM_NO']= row['CLM_NO']
            output_rows_new.append(new_row)
          
        old_row=row
    
    output_rows_new.sort(key=operator.itemgetter('CLM_AGRE_ID','INDST_CD_EFF_DT','INDST_CD_END_DT'))

###################if the end date of the old row and the start date of the new row are same ,
###################add a day to the new_row_start_date.   

    new_row=[]
    old_row=[]
   
    for row in output_rows_new:
        if not old_row:
            old_row=row
            continue
        
        if old_row['INDST_CD_END_DT']==row['INDST_CD_EFF_DT']:
            row['INDST_CD_EFF_DT']=old_row['INDST_CD_END_DT']+aday
        old_row=row
     
    output_rows_new.sort(key=operator.itemgetter('CLM_AGRE_ID','INDST_CD_EFF_DT','INDST_CD_END_DT'))

##########SMUSH AGAIN using postion(del the unwanted row based on position)
    new_row=[]
    old_row=[]
    end_day=datetime.datetime(9999,12,31,0,0)

    to_del=[]    
    for idx,row in enumerate(output_rows_new):
        if not old_row:
            old_row=row
            continue
        #continuous: move cur_ror eff date backwards to old row, and removing old row      
        if row['INDST_CD_EFF_DT']==old_row['INDST_CD_END_DT'] + aday and old_row['INDST_CD'] == row['INDST_CD'] and old_row['INDST_CD_END_DT'] !=end_day:
            row['INDST_CD_EFF_DT']=old_row['INDST_CD_EFF_DT']
            to_del.append(idx-1)
        old_row=row

    output_rows_new.sort(key=operator.itemgetter('CLM_AGRE_ID','INDST_CD_EFF_DT','INDST_CD_END_DT'))

    #delete unneeded rows
    for idx in to_del:
        del output_rows_new[idx]

    #force last row to be end_day
    output_rows_new[-1]['INDST_CD_END_DT'] =end_day        
    output_rows_new.sort(key=operator.itemgetter('CLM_AGRE_ID','INDST_CD_EFF_DT','INDST_CD_END_DT'))

    return output_rows_new

def print_row(row,fw=''):

    row_line2=''
    fields=['PLCY_AGRE_ID','CLM_AGRE_ID','CUST_ID','CLM_NO','PLCY_NO','PLCY_PRD_PTCP_EFF_DATE','PLCY_PRD_PTCP_END_DATE', 'CLM_PLCY_RLTNS_EFF_DATE','CLM_PLCY_RLTNS_END_DATE']
    for field in fields:
        row_line=str(row.get(field,'NA'))
        row_line=row_line.replace(' 00:00:00','')
        row_line2+='\t'+row_line
        if fw:fw.write(str(row_line)+'\t')
    if fw:fw.write('\n')
    row_line2='\t'.join(fields)+'\n'+row_line2
    return '\n'+row_line2


def get_batch( rows, col ):
    #row generator: pec_pes_rows,column:'CLM_AGRE_ID'
    #batch only has 1 element in it at most
    #example batch = AGRE_ID:[ row, row, row ]
    batch = {}
    for row in rows:
        #col_value = getattr( row, col )
        col_value = row[col]
        if col_value not in batch:
            if batch:
                #got new key, yield rows from old key
                old_rows = batch.popitem()[1]
                yield old_rows
                #batch is now empty
            batch[col_value] = []
        #always runs
        batch[col_value].append( row )    
    if batch:
        yield batch.popitem()[1]
    #print 'batch',batch



def smush_CTL_ELEM_rows( CTL_ELEM_rows ):
    smushed_CTL_ELEM_rows = []
    #smush for a control element
    for row in CTL_ELEM_rows:
        #print 'rorrrrrrrrrrwwwwwww',row
        # if row['CLM_PLCY_RLTNS_EFF_DT'] > row['CLM_PLCY_RLTNS_END_DT'] :
        if row['CLM_PLCY_RLTNS_EFF_DT'] > row['CLM_PLCY_RLTNS_END_DT'] :
            #print 'DROPPPING',row
            continue
        if not smushed_CTL_ELEM_rows:
            smushed_CTL_ELEM_rows.append( row )
            #print 'APPENDING',row
            continue
        smushed_CTL_ELEM_rows = smush_row2( smushed_CTL_ELEM_rows, row, 'CLM_PLCY_RLTNS_EFF_DT', 'CLM_PLCY_RLTNS_END_DT' )
    #process smushed CTL_ELEM rows
    return smushed_CTL_ELEM_rows

def smushrow2dict( smushed_row ):
    smushed_row_dict = smushed_row
    smushed_row_dict[ 'INDST_CD' ] = ''
    smushed_row_dict[ 'INDST_NM' ] = ''
    smushed_row_dict[ 'INDST_CD_EFF_DT' ] = ''
    smushed_row_dict[ 'INDST_CD_END_DT' ] = ''
    smushed_row_dict[ 'CRNT_IND '] = ''
    smushed_row_dict[ 'DW_CREATE_DTTM' ] = ''
    smushed_row_dict[ 'DW_UPDATE_DTTM' ] = ''

    return smushed_row_dict

###main code4 - smush the non-driver row

def smush_pec_pes_rows( rows, aday, driver_row_dict, output_rows ):
    '''
    [row CLM_AGRE_ID='7483316', CLM_PRFL_ANSW_TEXT='2400', SOC_MNL_CLS_TYPE='P' ....
    ##            pec_pes_rows_new.HIST_EFF_DTM = old_row['HIST_EFF_DTM']

    #row._replace(**{'a':7})
    '''

    smushed_rows = []
    old_row = []
    #print 'beforesmush', rows
    #print 'driver_row_dict',driver_row_dict
    for row in rows:
        #print 'row', row
        if not old_row:
##            if driver_row_dict['CLM_PLCY_RLTNS_EFF_DT'] >  row['HIST_EFF_DTM'] :
##                val= driver_row_dict['CLM_PLCY_RLTNS_EFF_DT']
##                row=row._replace(**{'HIST_EFF_DTM': val} )
##                #continue
            smushed_rows.append( row )
            #print 'everyrowsmushed_rows', smushed_rows
            old_row = row
            #print '000000000000driver_row_dict',driver_row_dict
            #print '0000000000000000000old_row',old_row
            continue
        ##
        if old_row['MOD_INDST_CODE'] == row['MOD_INDST_CODE']:
            if old_row['HIST_END_DTM'] == datetime.datetime(9999, 12, 31, 0, 0):
                max_hist_end_dtm = old_row['HIST_END_DTM'] - aday
            else:
                max_hist_end_dtm = old_row['HIST_END_DTM']

            if (old_row['HIST_END_DTM'] == row['HIST_EFF_DTM'] or max_hist_end_dtm + aday  == row['HIST_EFF_DTM']) and old_row['MINO_BGN_DATE'] == row['MINO_BGN_DATE'] and old_row['MINO_END_DATE'] == row['MINO_END_DATE']:
##                print 'hererererere'
##                pprint.pprint(old_row)
##                pprint.pprint(row)
##                print 'smushed_rows[-1]',smushed_rows[-1]
                del smushed_rows[-1]
                row['HIST_EFF_DTM'] = old_row['HIST_EFF_DTM']
                #print' modified row', row
            elif old_row['HIST_EFF_DTM'] == row['HIST_EFF_DTM'] and old_row['HIST_END_DTM'] == row['HIST_END_DTM'] and old_row['MINO_BGN_DATE'] == row['MINO_BGN_DATE'] and old_row['MINO_END_DATE'] == row['MINO_END_DATE']:
                
                del smushed_rows[-1]
                row['HIST_EFF_DTM'] = old_row['HIST_EFF_DTM']
        ##change 5/17
        elif old_row['HIST_END_DTM'] == row['HIST_EFF_DTM']:
            val=old_row['HIST_END_DTM'] - aday
            #print 'ehre1'
            #print val
            if smushed_rows[-1] == old_row :
                #print 'here2'
                #print old_row
                #print row
                old_row['HIST_END_DTM'] = val
                #print smushed_rows[-1]
                smushed_rows[-1]['HIST_END_DTM'] = val
                #print smushed_rows[-1]
            else:
            
                old_row['HIST_END_DTM'] = val
            
            
        old_row = row
        smushed_rows.append( row )
        #print 'everyrowsmushed_rows', smushed_rows
    
####change 5/14     
    if driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ] >  smushed_rows[0]['HIST_EFF_DTM'] and driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] >=  smushed_rows[0]['HIST_END_DTM']:
        if smushed_rows[0]['HIST_END_DTM'] >= driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]:
            val = driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
            ###chg 6/5
            if output_rows:
                last_row = output_rows[-1]
                if val < last_row[ 'INDST_CD_END_DT' ] :
                    val = last_row[ 'INDST_CD_END_DT' ] + aday
            smushed_rows[0]['HIST_EFF_DTM'] = val
            #raw_input('here')
            #print 'output_rows',output_rows
            #print smushed_rows[0],'smushed_rows[0]'

    #print 'ssssssssssssssssmush', smushed_rows


    return smushed_rows

###the main code 2 which process all the logic for the pec-pes-pa-si rows

def process_pec_pes_pa_si( args, driver_row_dict, pec_pes_rows, output_rows, template_row, aday, now ):
    '''
    build new output rows based on comparing a single driver row with group of pec_pes_rows

    '''
    end_day = datetime.datetime(9999, 12, 31, 0, 0) 
    
    start_day = datetime.datetime(1900, 1, 1, 0, 0)
    flag = ''
##    print 'ENTRY ENTRY'
##    print 'pec_pes_rows', pec_pes_rows
##    print '****************'
##    print 'driver_row_dict22222222',driver_row_dict
##    print '****************'
##    print '****************'
##    print 'output_rowsinnn',output_rows
##    print '****************'
##    print '****************'

    pec_pes_rows = smush_pec_pes_rows ( pec_pes_rows, aday, driver_row_dict, output_rows )
    
##    print 'smushed pec_pes_rows'
##    print 'pec_pes_rows', pec_pes_rows
##    print '****************'
    

    
   
    if driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] <  pec_pes_rows[0]['HIST_EFF_DTM']:
            new_row = copy.deepcopy( template_row )
        
            new_row[ 'INDST_CD' ] = '11'
            
            new_row[ 'INDST_NM' ] = 'UNKNOWN'
            new_row[ 'INDST_CD_EFF_DT' ] =driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
            new_row[ 'INDST_CD_END_DT' ] = pec_pes_rows[0]['HIST_EFF_DTM'] - aday
            
            new_row[ 'CRNT_IND' ] = ''
            new_row[ 'DW_CREATE_DTTM' ] = now
            new_row[ 'DW_UPDATE_DTTM' ] = now
            new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
            new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
            output_rows.append( new_row )
            
            last_row = output_rows[-1]
    elif driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ] <  pec_pes_rows[0]['HIST_EFF_DTM'] and driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] >=  pec_pes_rows[0]['HIST_EFF_DTM'] :
        #
            
             #if last_row['CLM_AGRE_ID'] != driver_row_dict['CLM_AGRE_ID']:
                new_row = copy.deepcopy( template_row )
            
                new_row[ 'INDST_CD' ] = '11'
                
                new_row[ 'INDST_NM' ] = 'UNKNOWN'
                
                #change 6/5
                if output_rows:
                    last_row = output_rows[-1]
                    #print 'hhhhhh',last_row
                    new_row[ 'INDST_CD_EFF_DT' ] = last_row[ 'INDST_CD_END_DT' ] + aday
                else:
                    new_row[ 'INDST_CD_EFF_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
                '''
                special revert change 6/6 - not needed code - here for testing
                new_row['INDST_CD_EFF_DT']=driver_row_dict['CLM_PLCY_RLTNS_EFF_DT']
                '''
                new_row[ 'INDST_CD_END_DT' ] = pec_pes_rows[0]['HIST_EFF_DTM']  - aday
                
                new_row[ 'CRNT_IND' ] = ''
                new_row[ 'DW_CREATE_DTTM' ] = now
                new_row[ 'DW_UPDATE_DTTM' ] = now
                new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                output_rows.append( new_row )
                
                last_row = output_rows[-1]


##        ####change 5/14   
##    if driver_row_dict['CLM_PLCY_RLTNS_EFF_DT'] >  pec_pes_rows[0]['HIST_EFF_DTM'] :
##        val= driver_row_dict['CLM_PLCY_RLTNS_EFF_DT']
##        pec_pes_rows[0]=pec_pes_rows[0]['_replace'](**{'HIST_EFF_DTM': val} )

    second_time = ''
    for pec_pes_row in  pec_pes_rows:

        if pec_pes_row['HIST_END_DTM'] <  pec_pes_row['MINO_BGN_DATE'] :
            
            continue
  
        if driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] < pec_pes_row['HIST_EFF_DTM'] :
            
            continue

        ##to handle when the non-driver row or pec row was not there for the first driver row
        
        if not second_time:
            second_time = 'y'
            #raw_input('jjjjjjjjjjjjjjjjjjjjjjj')
            if output_rows :
                last_row = output_rows[-1]
  
                if pec_pes_row['HIST_END_DTM'] <= last_row[ 'INDST_CD_END_DT' ]:
                    second_time = ''
            
                    ###chnage 5/29 added after 6/5 change
                    val = last_row[ 'INDST_CD_END_DT' ] + aday
                    pec_pes_row['HIST_EFF_DTM'] = val 

                    continue
            
        
        ###change 5/14
        if driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ] ==  pec_pes_row['HIST_EFF_DTM'] and driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] ==  pec_pes_row['HIST_END_DTM'] :
            if pec_pes_row['MINO_END_DATE'] == end_day and pec_pes_row['MINO_BGN_DATE'] == start_day:

                new_row = copy.deepcopy( template_row )
            
                new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                
                new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
                new_row[ 'INDST_CD_EFF_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
                new_row[ 'INDST_CD_END_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]
                
                new_row[ 'CRNT_IND' ] = ''
                new_row[ 'DW_CREATE_DTTM' ] = now
                new_row[ 'DW_UPDATE_DTTM' ] = now
                new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                output_rows.append( new_row )
                #print 'outttttttttt',output_rows
                
                last_row = output_rows[-1]
                continue


        if pec_pes_row['HIST_END_DTM'] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]:
            flag = 'y'
            new_row = copy.deepcopy( template_row )
            #print 'i am herere'

            if pec_pes_row['HIST_EFF_DTM'] >= driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ] and pec_pes_row['HIST_EFF_DTM'] <= driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]:
                new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['HIST_EFF_DTM']
            else:
                

                    
                    
                new_row = copy.deepcopy( template_row )
                new_row[ 'INDST_CD_EFF_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
                    
            new_row [ 'INDST_CD_END_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]

            new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
            new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']

            new_row[ 'CRNT_IND' ] = ''
            new_row[ 'DW_CREATE_DTTM' ] = now
            new_row[ 'DW_UPDATE_DTTM' ] = now
            new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
            new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
            if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
            if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
            output_rows.append( new_row )
            
            last_row = output_rows[-1]
            #pprint.pprint(output_rows)
            continue

        
        if len( output_rows ) >= 1:
            
            last_row = output_rows[-1]
            if last_row[ 'INDST_CD_END_DT' ] == datetime.datetime(9999, 12, 31, 0, 0):
                 last_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_EFF_DTM'] - aday

        else:
            if driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ] < pec_pes_row['HIST_EFF_DTM'] :
            
                new_row = copy.deepcopy( template_row )
            
                new_row[ 'INDST_CD' ] = '11'
                new_row[ 'INDST_NM' ] = 'UNKNOWN'
                new_row[ 'INDST_CD_EFF_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
                new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_EFF_DTM']  - aday
                #need to set to Y for 12/31/9999 for all ca    ses
                new_row[ 'CRNT_IND' ] = ''
                new_row[ 'DW_CREATE_DTTM' ] = now
                new_row[ 'DW_UPDATE_DTTM' ] = now
                new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                #raw_input('gggggggggggggo')
                if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
                output_rows.append( new_row )
                
                last_row=output_rows[-1]
            else:

                new_row=copy.deepcopy( template_row )
                new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
                new_row[ 'INDST_CD_EFF_DT' ] = driver_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
                new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM']
                #need to set to Y for 12/31/9999 for all ca    ses
                new_row[ 'CRNT_IND' ] = ''
                new_row[ 'DW_CREATE_DTTM' ] =now
                new_row[ 'DW_UPDATE_DTTM' ] =now
                new_row[ 'CLM_AGRE_ID' ] = pec_pes_row['CLM_AGRE_ID']
                new_row[ 'CLM_NO' ] = pec_pes_row['CLM_NO']
                if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
                
                #print 'outputttttttttttt',output_rows
                output_rows.append( new_row )
                #print 'hereeeee3'
       ####2/21/2018
                last_row = output_rows[-1]

                continue


        if last_row[ 'INDST_CD' ] == '11' :
            ####if any date gap between the last row and the next row (pes_pec row) - update the blank row date
            #print "#######check on the below code2"
            if pec_pes_row['HIST_EFF_DTM']  >= last_row[ 'INDST_CD_EFF_DT' ]:

                #print 'last_row',last_row
                
                last_row[ 'INDST_CD_EFF_DT' ] = last_row[ 'INDST_CD_EFF_DT' ] 
                last_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_EFF_DTM']  - aday


        ind_same = ( pec_pes_row['MOD_INDST_CODE'] == last_row[ 'INDST_CD' ])
     
        if last_row[ 'INDST_CD_END_DT' ] == end_day: #and pec_pes_row['HIST_END_DTM'] !=end_day:
            #print 'hereeeee4'
            continue
        
        pec_date_continous = ( pec_pes_row['HIST_EFF_DTM'] == last_row[ 'INDST_CD_END_DT' ] + aday ) 
        mino_good = ( pec_pes_row['MINO_BGN_DATE']  <= last_row[ 'INDST_CD_EFF_DT' ] and pec_pes_row['MINO_END_DATE']  >= last_row[ 'INDST_CD_END_DT' ] )

##        print pec_date_continous,'pec_date_continous'
        #print mino_good,'mino_good'
##        print ind_same,'ind_same'
##        raw_input('xx')        

        if pec_date_continous and ind_same and mino_good:
            
            last_row[ 'INDST_CD_EFF_DT' ] = last_row[ 'INDST_CD_EFF_DT' ] 
            last_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM']
            #print 'hereeeee5'
            #return output_rows
        elif pec_date_continous and not ind_same and mino_good:

                new_row = copy.deepcopy( template_row )
                new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
                
                new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['HIST_EFF_DTM']

                if pec_pes_row['HIST_END_DTM'] > pec_pes_row['MINO_END_DATE']:
                    new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['MINO_END_DATE']
                else:
                    new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM']


                new_row[ 'CTL_ELEM_SUB_TYP_CD' ] = driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                #print 'new_row',new_row
                if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
     
                output_rows.append( new_row )
                #print output_rows
   
        elif ( not pec_date_continous and ind_same and mino_good ) or ( not pec_date_continous and not ind_same and mino_good ): ###not a 11
                    ##create a dummy row
                    new_row = copy.deepcopy( template_row )
                    
                    new_row[ 'INDST_CD' ] = '11'
                    new_row[ 'INDST_NM' ] = 'UNKNOWN'
                    if pec_pes_row['HIST_END_DTM'] == end_day:
                        new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                        new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
                    if  not ind_same or ind_same:
       
                        new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                        new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']

                    new_row[ 'INDST_CD_EFF_DT' ] = last_row[ 'INDST_CD_END_DT' ] + aday
                    ####change 5/14/2018
                    ##new_row['INDST_CD_EFF_DT']= last_row['INDST_CD_END_DT']    
                    ##last_row['INDST_CD_END_DT']= last_row['INDST_CD_END_DT'] - aday


                    ###change 4/17
                    ####new_row['INDST_CD_END_DT']= pec_pes_row['HIST_EFF_DTM'] - aday
                    new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM'] 
                    new_row[ 'CTL_ELEM_SUB_TYP_CD' ] = driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                    new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                    new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                    #print 'new row'
                    #print new_row
                    if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                    if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue

                    output_rows.append( new_row )
                    continue
                
                    ###pec row ###CHANGE 4/17 START
##                    new_row=copy.deepcopy(template_row)
##                    new_row['INDST_CD']=pec_pes_row['MOD_INDST_CODE']
##                    new_row['INDST_NM']=pec_pes_row['MOD_INDST_DESC']
##                    new_row['INDST_CD_EFF_DT']= pec_pes_row['HIST_EFF_DTM']
##                    new_row['INDST_CD_END_DT']= pec_pes_row['HIST_END_DTM']
##                    new_row['CTL_ELEM_SUB_TYP_CD']=driver_row_dict['CTL_ELEM_SUB_TYP_CD']
##                    new_row['CLM_AGRE_ID']= driver_row_dict['CLM_AGRE_ID']
##                    new_row['CLM_NO']= driver_row_dict['CLM_NO']
##                    if new_row['INDST_CD_END_DT'] < new_row['INDST_CD_EFF_DT']: continue
##                    if new_row['INDST_CD_END_DT'] > driver_row_dict['CLM_PLCY_RLTNS_END_DT']: continue
##                    
##                    output_rows.append(new_row)
                    ###CHANGE 4/17 END
    ##                    if pec_pes_row['HIST_END_DTM'] != end_day: ###do we have to do this?
    ##                        print pec_pes_row['HIST_END_DTM']
    ##                        raw_input('a3')
    ##                        new_row=copy.deepcopy(template_row)
    ##                        new_row['INDST_CD']='110'
    ##                        new_row['INDST_NM']='UNKNOWN'
    ##                        new_row['INDST_CD_EFF_DT']= pec_pes_row['HIST_END_DTM'] + aday
    ##                        new_row['INDST_CD_END_DT']= end_day #pec_pes_row['HIST_END_DTM']
    ##                        new_row['CTL_ELEM_SUB_TYP_CD']=driver_row_dict['CTL_ELEM_SUB_TYP_CD']
    ##                        new_row['CLM_AGRE_ID']= driver_row_dict['CLM_AGRE_ID']
    ##                        new_row['CLM_NO']= driver_row_dict['CLM_NO']
    ##                        if new_row['INDST_CD_END_DT'] < new_row['INDST_CD_EFF_DT']: continue
    ##                        output_rows.append(new_row)  
                
        elif  ( pec_date_continous and ind_same and not mino_good ) or ( not pec_date_continous and not ind_same and not mino_good ) or ( not pec_date_continous and ind_same and not mino_good ) or ( pec_date_continous and not ind_same and not mino_good ):
              
                
                if pec_pes_row['MINO_BGN_DATE'] > last_row[ 'INDST_CD_EFF_DT' ] :
                    #print 'yellow'    
                    new_row = copy.deepcopy( template_row )
                 
                    new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                    new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
      
                    if pec_pes_row['MINO_BGN_DATE'] > pec_pes_row['HIST_EFF_DTM'] and pec_pes_row['MINO_BGN_DATE'] < pec_pes_row['HIST_END_DTM'] :
                    #if pec_pes_row['HIST_EFF_DTM'] < last_row['INDST_CD_END_DT'] :
                        #new_row['INDST_CD_EFF_DT']= last_row['INDST_CD_END_DT'] + aday
                        new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['MINO_BGN_DATE']
                        new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM']
                        ####change 3/22/2018
                        last_row[ 'INDST_CD_END_DT' ] = pec_pes_row['MINO_BGN_DATE'] -aday
                    
                    else:
                        new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['HIST_EFF_DTM']
                        new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['MINO_BGN_DATE'] - aday
                     
                    
                    new_row[ 'CTL_ELEM_SUB_TYP_CD' ] = driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                    new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                    new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
         
                    
                    if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                    if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
                    
                    output_rows.append( new_row )
                

                    new_row=copy.deepcopy( template_row )
                    new_row[ 'INDST_CD' ] =pec_pes_row['MOD_INDST_CODE']
                    new_row[ 'INDST_NM' ] =pec_pes_row['MOD_INDST_DESC']
                    new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['MINO_BGN_DATE']
                    new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['MINO_END_DATE']
                    new_row[ 'CTL_ELEM_SUB_TYP_CD' ] =driver_row_dict['CTL_ELEM_SUB_TYP_CD']
                    new_row[ 'CLM_AGRE_ID' ] = driver_row_dict['CLM_AGRE_ID']
                    new_row[ 'CLM_NO' ] = driver_row_dict['CLM_NO']
                    if new_row['INDST_CD_END_DT'] < new_row['INDST_CD_EFF_DT']: continue
                    if new_row['INDST_CD_END_DT'] > driver_row_dict['CLM_PLCY_RLTNS_END_DT']: continue
                    output_rows.append( new_row )
                    if pec_pes_row['MINO_END_DATE']  < last_row[ 'INDST_CD_END_DT' ]:
                        #raw_input('a1')
                        new_row = copy.deepcopy( template_row )
                        new_row[ 'INDST_CD' ] = '11'
                        #raw_input('a3')
                        new_row[ 'INDST_NM']='UNKNOWN'
                        new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['MINO_END_DATE'] + aday
                        new_row[ 'INDST_CD_END_DT' ] = last_row[ 'INDST_CD_END_DT' ] #pec_pes_row['HIST_END_DTM']
                        new_row[ 'CTL_ELEM_SUB_TYP_CD' ] = driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                        new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                        new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                        if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                        if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
                        output_rows.append( new_row )
                    
                else:
                    if pec_pes_row['MINO_END_DATE']  < last_row[ 'INDST_CD_END_DT' ]:
                      
                        new_row = copy.deepcopy( template_row )
                        new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                        new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
                        new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['HIST_EFF_DTM']
                        new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['MINO_END_DATE']
                        new_row[ 'CTL_ELEM_SUB_TYP_CD' ] =driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                        new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                        new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                
                        if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                        if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
                        output_rows.append( new_row )

                        new_row=copy.deepcopy( template_row )
                  
                        new_row[ 'INDST_CD' ] = '11'
                        new_row[ 'INDST_NM' ] = 'UNKNOWN'
                        new_row[ 'INDST_CD_EFF_DT' ] = pec_pes_row['MINO_END_DATE'] + aday
                        new_row[ 'INDST_CD_END_DT' ] = last_row[ 'INDST_CD_END_DT' ] #pec_pes_row['HIST_END_DTM']
                        new_row[ 'CTL_ELEM_SUB_TYP_CD' ] = driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                        new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                        new_row[ 'CLM_NO' ] = driver_row_dict[ 'CLM_NO' ]
                

                        if new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ]: continue
                        if new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: continue
                        
                        output_rows.append( new_row )                        

         
        else:
            raise ValueError( 'one condition missedin pec/pes combo' )



    #To handle one special condition of (not pec_date_continous and ind_same and mino_good) or (not pec_date_continous and not ind_same and mino_good)

    if len( output_rows ) >= 1 and flag != 'y':
    ##        if not (driver_row_dict['CLM_PLCY_RLTNS_END_DT'] < pec_pes_row['HIST_EFF_DTM'] or driver_row_dict['CLM_PLCY_RLTNS_EFF_DT'] > pec_pes_row['HIST_END_DTM']):
        #raw_input('len(output_rows) >= 1 and flag ==''')
        last_row = output_rows[-1]
  
        if last_row[ 'INDST_CD_END_DT' ] != end_day and pec_pes_row['MOD_INDST_CODE'] == last_row[ 'INDST_CD' ]:
            #and driver_row_dict['CLM_PLCY_RLTNS_EFF_DT'] > pec_pes_row['HIST_EFF_DTM'] ): ###do we have to do this?

            if last_row[ 'INDST_CD_END_DT' ] > pec_pes_row['HIST_END_DTM'] :
                last_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM']
            

        elif last_row[ 'INDST_CD_END_DT' ] != end_day and pec_pes_row['MOD_INDST_CODE'] != last_row[ 'INDST_CD' ]:
     
            if driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] > pec_pes_row['HIST_EFF_DTM'] :
           
    
                new_row = copy.deepcopy( template_row )
                new_row[ 'INDST_CD' ] = pec_pes_row['MOD_INDST_CODE']
                new_row[ 'INDST_NM' ] = pec_pes_row['MOD_INDST_DESC']
                new_row[ 'INDST_CD_EFF_DT' ] = last_row[ 'INDST_CD_END_DT' ] + aday
                new_row[ 'INDST_CD_END_DT' ] = pec_pes_row['HIST_END_DTM'] #end_day
                new_row[ 'CTL_ELEM_SUB_TYP_CD' ] = driver_row_dict[ 'CTL_ELEM_SUB_TYP_CD' ]
                new_row[ 'CLM_AGRE_ID' ] = driver_row_dict[ 'CLM_AGRE_ID' ]
                new_row[ 'CLM_NO'] = driver_row_dict[ 'CLM_NO' ]
                if not new_row[ 'INDST_CD_END_DT' ] > driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]: 
                    if not ( new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ] ):
                        output_rows.append( new_row )
    #print 'herererere'

    
    
    if pec_pes_row['HIST_END_DTM'] == end_day and driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] == end_day:

        last_row = output_rows[-1]
        last_row[ 'INDST_CD_END_DT' ] = end_day
    else:
        last_row = output_rows[-1]
        if driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] < last_row[ 'INDST_CD_END_DT' ]:
            last_row[ 'INDST_CD_END_DT' ] =  driver_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ] 
    #raw_input('go')
    

    return output_rows 
    


def get_next( gen_obj, rowtype = '' ):
    try:
        #raw_input('before next:'+rowtype)
        val = next( gen_obj )
        #print '\t------------','get_next',rowtype
        #print '\tval=',val[0]['CLM_AGRE_ID']
        #raw_input('finished get next:'+rowtype)
        return val
    except StopIteration:
        pass
        return []

###the main code which process all the rows

def process_dbrows( args, driver_rows, pec_pes_rows, pa_rows, other_rows, errors, senario=False, log_obj = None ):
    '''
    layers of batching are used to ensure date smushing is only used for a small enough subset
    get_batch(driver_rows,'CLM_AGRE_ID')
        get_batch(driver_batch_rows,'CTL_ELEM_SUB_TYP_CD')
            smush_CTL_ELEM_rows
            'pes','pec' are all converted pec, if not categorized make si
            smushing: previous rows are kept in a args.cache, so new rows can be smushed with them

    '''
    #yield groups of rows as a list by CLM_AGRE_ID
    pec_pes_rows = remove_duplicates( get_batch( pec_pes_rows, 'CLM_AGRE_ID' ) )
    pa_rows = remove_duplicates( get_batch( pa_rows, 'CLM_AGRE_ID' ) )
    other_rows = remove_duplicates( get_batch( other_rows, 'CLM_AGRE_ID' ) )
    #
    pa_batch = get_next( pa_rows, 'pa' )
    print(f'pa_batch = {pa_batch}')

    pec_pes_batch = get_next( pec_pes_rows, 'pec_pes' )
    other_batch = get_next( other_rows, 'other' )

    #STANDARD for all
    aday =  datetime.timedelta( days = 1 )
    end_day = datetime.datetime( 9999, 12, 31, 0, 0 )
    now = datetime.datetime.now()
    template_row = {'CLM_AGRE_ID': '', 'CLM_NO': '', 'INDST_CD':'', 'INDST_NM':'', 'INDST_CD_EFF_DT':'', 
                    'INDST_CD_END_DT':'', 'CRNT_IND':'', 'DW_CREATE_DTTM':now, 'DW_UPDATE_DTTM':now, 'CTL_ELEM_SUB_TYP_CD':'',
                   }

    for driver_batch_rows in get_batch( driver_rows, 'CLM_AGRE_ID' ):
        output_rows = []
        
        ## batch the policy types together so that we can smush and they would be continuous.
        args.cache = {}
        next_batch = 'y'
        x = ()

        find = ''
        #find='1000076005'
        #find= '3136717'
        #find='11198667'  
        for driver_CTL_ELEM_rows in get_batch( driver_batch_rows, 'CTL_ELEM_SUB_TYP_CD' ):
        #for driver_CTL_ELEM_rows in driver_batch_rows:

            smushed_CTL_ELEM_rows = smush_CTL_ELEM_rows( driver_CTL_ELEM_rows )
           
            #identify type of row
            if not smushed_CTL_ELEM_rows : continue
            smush_agid = smushed_CTL_ELEM_rows[0]['CLM_AGRE_ID']
            smush_type = smushed_CTL_ELEM_rows[0]['CTL_ELEM_SUB_TYP_CD']


            #change 2/21             
            ##args.cachekey=(smush_agid,smush_type)
            if smush_type in ( 'pes', 'pec' ):
                smush_type = 'pec'
            elif smush_type == ( 'pa' ):
                smush_type = 'pa'
            else: 
                smush_type = 'si'
            
            args.cachekey = ( smush_agid, smush_type )
            
            #print 'driver',  smush_agid, smush_type,'pes_pec',len(pec_pes_batch),'pa',len(pa_batch),'other',len(other_batch)
            #print 'smushed_CTL_ELEM_rows-dict22222',smushed_CTL_ELEM_rows

            for smushed_row in smushed_CTL_ELEM_rows:
##                if find and smush_agid == find:
##                    print 'args.cache2**********'
##                    pprint.pprint(args.cache)

##                if find and smush_agid == find:                
##                    print '$$driver',smushed_row['CLM_AGRE_ID']
##                    if pa_batch: print '\t pa=',len(pa_batch),pa_batch[0]['CLM_AGRE_ID'], pa_batch[0]
##                    if pec_pes_batch: print '\t pec=',len(pec_pes_batch),pec_pes_batch[0]['CLM_AGRE_ID'],pec_pes_batch[0]
##                    if other_batch: print '\t other=',len(other_batch),other_batch[0]['CLM_AGRE_ID'],other_batch[0]           
##                    raw_input('go')
                smushed_row_dict = smushrow2dict( smushed_row )


                ##change 2/21/2018 
                #unx=(smushed_row['CLM_AGRE_ID'],smushed_row['CTL_ELEM_SUB_TYP_CD'])

                the_key = smushed_row['CTL_ELEM_SUB_TYP_CD']
                if the_key in ( 'pes', 'pec' ):
                    the_key = 'pec'
                elif the_key == ( 'pa' ):
                    the_key = 'pa'
                else:
                    the_key = 'si'

                unx = ( smushed_row[ 'CLM_AGRE_ID' ], the_key )   #unique key for the args.cache     

                if ( pec_pes_batch and pec_pes_batch[0]['CLM_AGRE_ID'] == smush_agid and smush_type in ( 'pec', 'pes' ) ) or ( args.cachekey in args.cache and smush_type in ( 'pec', 'pes' ) ):
                    
                    if args.cachekey in args.cache:
                        ##print 'in args.cache pec pes batch'
                        x = pec_pes_batch  
                        pec_pes_batch = args.cache[ unx ]
                        output_rows = process_pec_pes_pa_si(args, smushed_row_dict, pec_pes_batch, output_rows, template_row, aday, now )
                        pec_pes_batch = x
                    if pec_pes_batch and pec_pes_batch[0]['CLM_AGRE_ID'] == smush_agid and smush_type in ( 'pec', 'pes' ) :
                        args.cache[unx] = pec_pes_batch 
                        ##print "pec pes batch args.cache", args.cache
                        output_rows = process_pec_pes_pa_si(args, smushed_row_dict, pec_pes_batch, output_rows, template_row, aday, now )
                        pec_pes_batch = get_next( pec_pes_rows, 'pec_pes' )
                    
                elif ( pa_batch and pa_batch[0]['CLM_AGRE_ID'] == smush_agid and smush_type == ( 'pa' ) ) or ( args.cachekey in args.cache and smush_type == ( 'pa' ) ):
              
                    if args.cachekey in args.cache:
                        x = pa_batch
                        pa_batch = args.cache[unx]
                        output_rows = process_pec_pes_pa_si(args, smushed_row_dict, pa_batch, output_rows, template_row, aday, now )
                        pa_batch = x
 
                    if pa_batch and pa_batch[0]['CLM_AGRE_ID'] == smush_agid and smush_type == ( 'pa' ) :
                        args.cache[unx] = pa_batch
                        ##print "pa batch args.cache", args.cache
                        output_rows = process_pec_pes_pa_si( args, smushed_row_dict, pa_batch, output_rows, template_row, aday, now )
                        pa_batch = get_next( pa_rows, 'pa' )
   
                
                elif ( other_batch and other_batch[0]['CLM_AGRE_ID'] == smush_agid and smush_type not in ( 'pa', 'pec', 'pes' ) ) or ( args.cachekey in args.cache and smush_type not in ( 'pa', 'pec', 'pes' ) ):
                  
                    if args.cachekey in args.cache:
                        x = other_batch
                        other_batch = args.cache[unx]
                        output_rows = process_pec_pes_pa_si( args, smushed_row_dict, other_batch, output_rows, template_row, aday, now )
                        other_batch = x
                    
                    if other_batch and other_batch[0]['CLM_AGRE_ID'] == smush_agid and smush_type not in ( 'pa', 'pec', 'pes' ):
                        args.cache[unx] = other_batch
                        ##print "other batch args.cache", args.cache
                        output_rows = process_pec_pes_pa_si( args, smushed_row_dict, other_batch, output_rows, template_row, aday, now )
                        other_batch = get_next( other_rows, 'other' )

                else:
                    new_row = copy.deepcopy( template_row ) 
                
                    new_row[ 'INDST_CD' ] = '11'
                    new_row[ 'INDST_NM' ] = 'UNKNOWN'
                    new_row[ 'INDST_CD_EFF_DT' ] = smushed_row_dict[ 'CLM_PLCY_RLTNS_EFF_DT' ]
                    new_row[ 'INDST_CD_END_DT' ] = smushed_row_dict[ 'CLM_PLCY_RLTNS_END_DT' ]

                    new_row[ 'CRNT_IND' ] = ''
                    new_row[ 'DW_CREATE_DTTM' ] = now
                    new_row[ 'DW_UPDATE_DTTM' ] = now
                    new_row[ 'CLM_AGRE_ID' ] = smushed_row_dict['CLM_AGRE_ID']
                    new_row[ 'CLM_NO' ] = smushed_row_dict[ 'CLM_NO' ]
                    if not ( new_row[ 'INDST_CD_END_DT' ] < new_row[ 'INDST_CD_EFF_DT' ] ):
                        output_rows.append(new_row)                 
        try:
            if output_rows:
                output_rows = smush_row_indst( output_rows, 'INDST_CD_EFF_DT', 'INDST_CD_END_DT', 'INDST_CD', template_row, now )
            yield output_rows                 
            output_rows = []
        except:
            errors.append( output_rows )
            output_rows = []
            ##email
            yield []
            

       
def get_senarios( minid = '', maxid = '' ):

    senarios = {}

    if minid or maxid:
        if not maxid: maxid = 99999999999999999
        senarios[0]={
            'sql1':'WHERE A.CLM_AGRE_ID>=%s AND A.CLM_AGRE_ID<=%s'%(minid,maxid),
            'sql2':'AND CPRH.AGRE_ID>=%s AND CPRH.AGRE_ID<=%s'%(minid,maxid),
            'sql3':'AND CCCH.CLM_AGRE_ID>=%s AND CCCH.CLM_AGRE_ID<=%s'%(minid,maxid),
            'sql4':'AND CCCH.CLM_AGRE_ID>=%s AND CCCH.CLM_AGRE_ID<=%s'%(minid,maxid),
            }
    else:
        senarios[0]={
            'sql1':'',
            'sql2':'',
            'sql3':'',
            'sql4':'',
            }

    '''
    test case 1 should give 146? rows
    '''
    senarios[1]={
        'sql1':"WHERE A.CLM_NO IN ('18-135381','07-871347','12-331303','05-878010','00-300105','04-834131','01-867930','00-300105','16-341346','05-832794','00-405179','05-854820','00-300434','07-394699','02-867985','00-306885','00-300296','00-375325','16-315432','94-302008','07-394699','06-877032','04-841788','02-845847','10-838328','08-344930','12-838891','10-828913','13-822654','11-804150','16-112158','2-448261','13-828764','00-303346','00-301150','04-864924','06-894041','RL63','13-851047','94-319673','99-603990','99-603900','10-304245','01-836404','00-301491','00-308247','00-300004','01-319117','02-860877','13-345378','03-874809','00-438584','00-300004','17-163688','07-376724','00-303860','00-375325','00-300013','93-313317','PE648942')",
        'sql2':"AND A.CLM_NO IN ('18-135381','07-871347','12-331303','05-878010','00-300105','04-834131','01-867930','00-300105','16-341346','05-832794','00-405179','05-854820','00-300434','07-394699','02-867985','00-306885','00-300296','00-375325','16-315432','94-302008','07-394699','06-877032','04-841788','02-845847','10-838328','08-344930','12-838891','10-828913','13-822654','11-804150','16-112158','2-448261','13-828764','00-303346','00-301150','04-864924','06-894041','RL63','13-851047','94-319673','99-603990','99-603900','10-304245','01-836404','00-301491','00-308247','00-300004','01-319117','02-860877','13-345378','03-874809','00-438584','00-300004','17-163688','07-376724','00-303860','00-375325','00-300013','93-313317','PE648942')",
        'sql3':"AND A.CLM_NO IN ('18-135381','07-871347','12-331303','05-878010','00-300105','04-834131','01-867930','00-300105','16-341346','05-832794','00-405179','05-854820','00-300434','07-394699','02-867985','00-306885','00-300296','00-375325','16-315432','94-302008','07-394699','06-877032','04-841788','02-845847','10-838328','08-344930','12-838891','10-828913','13-822654', '11-804150','16-112158','2-448261','13-828764','00-303346','00-301150','04-864924','06-894041','RL63','13-851047','94-319673','99-603990','99-603900','10-304245','01-836404','00-301491','00-308247','00-300004','01-319117','02-860877','13-345378','03-874809','00-438584','00-300004','17-163688','07-376724','00-303860','00-375325','00-300013','93-313317','PE648942')",
        'sql4':"AND A.CLM_NO IN ('18-135381','07-871347','12-331303','05-878010','00-300105','04-834131','01-867930','00-300105','16-341346','05-832794','00-405179','05-854820','00-300434','07-394699','02-867985','00-306885','00-300296','00-375325','16-315432','94-302008','07-394699','06-877032','04-841788','02-845847','10-838328','08-344930','12-838891','10-828913','13-822654','11-804150','16-112158','2-448261','13-828764','00-303346','00-301150','04-864924','06-894041','RL63','13-851047','94-319673','99-603990','99-603900','10-304245','01-836404','00-301491','00-308247','00-300004','01-319117','02-860877','13-345378','03-874809','00-438584','00-300004','17-163688','07-376724','00-303860','00-375325','00-300013','93-313317','PE648942')",
        }
    senarios[2]={
        'sql1':"WHERE A.CLM_NO IN ('00-308247')",
        'sql2':"AND A.CLM_NO IN ('00-308247')",
        'sql3':"AND A.CLM_NO IN ('00-308247')",
        'sql4':"AND A.CLM_NO IN ('00-308247')",
        }
    senarios[3]={
        'sql1':"WHERE A.CLM_NO IN ('00-308247','00-300004','03-849130')",
        'sql2':"AND A.CLM_NO IN ('00-308247','00-300004','03-849130')",
        'sql3':"AND A.CLM_NO IN ('00-308247','00-300004','03-849130')",
        'sql4':"AND A.CLM_NO IN ('00-308247','00-300004','03-849130')",
        }
    senarios[4]={
        'sql1':"WHERE A.CLM_NO IN ('13-345378')",
        'sql2':"AND A.CLM_NO IN ('13-345378')",
        'sql3':"AND A.CLM_NO IN ('13-345378')",
        'sql4':"AND A.CLM_NO IN ('13-345378')",
        }
    senarios[5]={
        'sql1':"WHERE A.CLM_NO IN ('03-313127')",
        
        'sql2':"AND A.CLM_NO IN ('03-313127')",
        'sql3':"AND A.CLM_NO IN ('03-313127')",
        'sql4':"AND A.CLM_NO IN ('03-313127')",
        }
    senarios[6]={
        'sql1':"WHERE A.CLM_NO IN ('02-814260')",
        'sql2':"AND A.CLM_NO IN ('02-814260')",
        'sql3':"AND A.CLM_NO IN ('02-814260')",
        'sql4':"AND A.CLM_NO IN ('02-814260')",
        }
    senarios[7]={
        'sql1':"WHERE A.CLM_NO IN ('13-851047','94-319673')",
      
        'sql2':"AND A.CLM_NO IN ('13-851047','94-319673')",
        'sql3':"AND A.CLM_NO IN ('13-851047','94-319673')",
        'sql4':"AND A.CLM_NO IN ('13-851047','94-319673')",
        }
    senarios[8]={
        'sql1':"WHERE A.CLM_NO IN ('06-894041','RL63')",
        'sql2':"AND A.CLM_NO IN ('06-894041','RL63')",
        'sql3':"AND A.CLM_NO IN ('06-894041','RL63')",
        'sql4':"AND A.CLM_NO IN ('06-894041','RL63')",
        }
    senarios[9]={
        'sql1':"WHERE A.CLM_NO IN ('13-345378','RL63','04-864924')",
        'sql2':"AND A.CLM_NO IN ('13-345378','RL63','04-864924')",
        'sql3':"AND A.CLM_NO IN ('13-345378','RL63','04-864924')",
        'sql4':"AND A.CLM_NO IN ('13-345378','RL63','04-864924')",
        }
    senarios[10]={
        'sql1':"WHERE A.CLM_NO IN ('10-838936')",
        'sql2':"AND A.CLM_NO IN ('10-838936')",
        'sql3':"AND A.CLM_NO IN ('10-838936')",
        'sql4':"AND A.CLM_NO IN ('10-838936')",
        }
    senarios[11]={
        'sql1':"WHERE A.CLM_NO IN ('13-828764','00-301094')",
        'sql2':"AND A.CLM_NO IN ('13-828764','00-301094')",
        'sql3':"AND A.CLM_NO IN ('13-828764','00-301094')",
        'sql4':"AND A.CLM_NO IN ('13-828764','00-301094')",
        }
    senarios[12]={
        'sql1':"WHERE A.CLM_NO IN ('11-804150','13-851047')",
        'sql2':"AND A.CLM_NO IN ('11-804150','13-851047')",
        'sql3':"AND A.CLM_NO IN ('11-804150','13-851047')",
        'sql4':"AND A.CLM_NO IN ('11-804150','13-851047')",
        }
    senarios[13]={
        'sql1':"WHERE A.CLM_NO IN ('13-822654')",
        'sql2':"AND A.CLM_NO IN ('13-822654')",
        'sql3':"AND A.CLM_NO IN ('13-822654')",
        'sql4':"AND A.CLM_NO IN ('13-822654')",
        }

    senarios[14]={
        'sql1':"WHERE A.CLM_NO IN ('12-838891','10-828913','13-345378','06-894041','08-344930','10-838328')",
        'sql2':"AND A.CLM_NO IN ('12-838891','10-828913','13-345378','06-894041','08-344930','10-838328')",
        'sql3':"AND A.CLM_NO IN ('12-838891','10-828913','13-345378','06-894041','08-344930','10-838328')",
        'sql4':"AND A.CLM_NO IN ('12-838891','10-828913','13-345378','06-894041','08-344930','10-838328')",
        }

    senarios[15]={
        'sql1':"WHERE A.CLM_NO IN ('02-845847')",
        'sql2':"AND A.CLM_NO IN ('02-845847')",
        'sql3':"AND A.CLM_NO IN ('02-845847')",
        'sql4':"AND A.CLM_NO IN ('02-845847')",
        }

    senarios[16]={
        'sql1':"WHERE A.CLM_NO IN ('04-841788')",
        'sql2':"AND A.CLM_NO IN ('04-841788')",
        'sql3':"AND A.CLM_NO IN ('04-841788')",
        'sql4':"AND A.CLM_NO IN ('04-841788')",
        }

    senarios[17]={
        'sql1':"WHERE A.CLM_NO IN ('12-838891')",
        'sql2':"AND A.CLM_NO IN ('12-838891')",
        'sql3':"AND A.CLM_NO IN ('12-838891')",
        'sql4':"AND A.CLM_NO IN ('12-838891')",
        }

    senarios[18]={
        'sql1':"WHERE A.CLM_NO IN ('94-352988')",
        'sql2':"AND A.CLM_NO IN ('94-352988')",
        'sql3':"AND A.CLM_NO IN ('94-352988')",
        'sql4':"AND A.CLM_NO IN ('94-352988')",
        }

    senarios[19]={
        'sql1':"WHERE A.CLM_NO IN ('06-877032')",
        'sql2':"AND A.CLM_NO IN ('06-877032')",
        'sql3':"AND A.CLM_NO IN ('06-877032')",
        'sql4':"AND A.CLM_NO IN ('06-877032')",
        }

    senarios[20]={
        'sql1':"WHERE A.CLM_NO IN ('02-823138')",
        'sql2':"AND A.CLM_NO IN ('02-823138')",
        'sql3':"AND A.CLM_NO IN ('02-823138')",
        'sql4':"AND A.CLM_NO IN ('02-823138')",
        }

    senarios[21]={
        'sql1':"WHERE A.CLM_NO IN ('94-302008','07-394699')",
        'sql2':"AND A.CLM_NO IN ('94-302008','07-394699')",
        'sql3':"AND A.CLM_NO IN ('94-302008','07-394699')",
        'sql4':"AND A.CLM_NO IN ('94-302008','07-394699')",
        }

    senarios[22]={
        'sql1':"WHERE A.CLM_NO IN ('16-315432','06-366266')",
        'sql2':"AND A.CLM_NO IN ('16-315432','06-366266')",
        'sql3':"AND A.CLM_NO IN ('16-315432','06-366266')",
        'sql4':"AND A.CLM_NO IN ('16-315432','06-366266')",
        }

    senarios[23]={
        'sql1':"WHERE A.CLM_NO IN ('00-316179','00-323125')",
        'sql2':"AND A.CLM_NO IN ('00-316179','00-323125')",
        'sql3':"AND A.CLM_NO IN ('00-316179','00-323125')",
        'sql4':"AND A.CLM_NO IN ('00-316179','00-323125')",
        }

    senarios[24]={
        'sql1':"WHERE A.CLM_NO IN ('00-375325')",
        'sql2':"AND A.CLM_NO IN ('00-375325')",
        'sql3':"AND A.CLM_NO IN ('00-375325')",
        'sql4':"AND A.CLM_NO IN ('00-375325')",
        }

    ###ADD THIS
    senarios[25]={
        'sql1':"WHERE A.CLM_NO IN ('00-300296')",
        'sql2':"AND A.CLM_NO IN ('00-300296')",
        'sql3':"AND A.CLM_NO IN ('00-300296')",
        'sql4':"AND A.CLM_NO IN ('00-300296')",
        }
    
    senarios[26]={
        'sql1':"WHERE A.CLM_NO IN ('07-394699')",
        'sql2':"AND A.CLM_NO IN ('07-394699')",
        'sql3':"AND A.CLM_NO IN ('07-394699')",
        'sql4':"AND A.CLM_NO IN ('07-394699')",
        }

    senarios[27]={
        'sql1':"WHERE A.CLM_NO IN ('00-300105')",
        'sql2':"AND A.CLM_NO IN ('00-300105')",
        'sql3':"AND A.CLM_NO IN ('00-300105')",
        'sql4':"AND A.CLM_NO IN ('00-300105')",
        }

    senarios[28]={
        'sql1':"WHERE A.CLM_NO IN ('00-300434')",
        'sql2':"AND A.CLM_NO IN ('00-300434')",
        'sql3':"AND A.CLM_NO IN ('00-300434')",
        'sql4':"AND A.CLM_NO IN ('00-300434')",
        }

    senarios[29]={
        'sql1':"WHERE A.CLM_NO IN ('00-302493')",
        'sql2':"AND A.CLM_NO IN ('00-302493')",
        'sql3':"AND A.CLM_NO IN ('00-302493')",
        'sql4':"AND A.CLM_NO IN ('00-302493')",
        }

    ###add this
    senarios[30]={
        'sql1':"WHERE A.CLM_NO IN ('00-306885')",
        'sql2':"AND A.CLM_NO IN ('00-306885')",
        'sql3':"AND A.CLM_NO IN ('00-306885')",
        'sql4':"AND A.CLM_NO IN ('00-306885')",
        }

    ##add this
    senarios[31]={
        'sql1':"WHERE A.CLM_NO IN ('02-867985')",
        'sql2':"AND A.CLM_NO IN ('02-867985')",
        'sql3':"AND A.CLM_NO IN ('02-867985')",
        'sql4':"AND A.CLM_NO IN ('02-867985')",
        }

    senarios[32]={
        'sql1':"WHERE A.CLM_NO IN ('05-854820')",
        'sql2':"AND A.CLM_NO IN ('05-854820')",
        'sql3':"AND A.CLM_NO IN ('05-854820')",
        'sql4':"AND A.CLM_NO IN ('05-854820')",
        }

    senarios[33]={
        'sql1':"WHERE A.CLM_NO IN ('00-405179')",
        'sql2':"AND A.CLM_NO IN ('00-405179')",
        'sql3':"AND A.CLM_NO IN ('00-405179')",
        'sql4':"AND A.CLM_NO IN ('00-405179')",
        }

    senarios[34]={
        'sql1':"WHERE A.CLM_NO IN ('05-832794')",
        'sql2':"AND A.CLM_NO IN ('05-832794')",
        'sql3':"AND A.CLM_NO IN ('05-832794')",
        'sql4':"AND A.CLM_NO IN ('05-832794')",
        }

    senarios[35]={
        'sql1':"WHERE A.CLM_NO IN ('05-832794')",
        'sql2':"AND A.CLM_NO IN ('05-832794')",
        'sql3':"AND A.CLM_NO IN ('05-832794')",
        'sql4':"AND A.CLM_NO IN ('05-832794')",
        }

    senarios[36]={
        'sql1':"WHERE A.CLM_NO IN ('00-300105')",
        'sql2':"AND A.CLM_NO IN ('00-300105')",
        'sql3':"AND A.CLM_NO IN ('00-300105')",
        'sql4':"AND A.CLM_NO IN ('00-300105')",
        }

    senarios[37]={
        'sql1':"WHERE A.CLM_NO IN ('00-302664')",
        'sql2':"AND A.CLM_NO IN ('00-302664')",
        'sql3':"AND A.CLM_NO IN ('00-302664')",
        'sql4':"AND A.CLM_NO IN ('00-302664')",
        }

    senarios[38]={
        'sql1':"WHERE A.CLM_NO IN ('02-887503')",
        'sql2':"AND A.CLM_NO IN ('02-887503')",
        'sql3':"AND A.CLM_NO IN ('02-887503')",
        'sql4':"AND A.CLM_NO IN ('02-887503')",
        }

    senarios[39]={
        'sql1':"WHERE A.CLM_NO IN ('09-830596')",
        'sql2':"AND A.CLM_NO IN ('09-830596')",
        'sql3':"AND A.CLM_NO IN ('09-830596')",
        'sql4':"AND A.CLM_NO IN ('09-830596')",
        }

    senarios[40]={
        'sql1':"WHERE A.CLM_NO IN ('16-341346')",
        'sql2':"AND A.CLM_NO IN ('16-341346')",
        'sql3':"AND A.CLM_NO IN ('16-341346')",
        'sql4':"AND A.CLM_NO IN ('16-341346')",
        }

    senarios[41]={
        'sql1':"WHERE A.CLM_NO IN ('00-300105')",
        'sql2':"AND A.CLM_NO IN ('00-300105')",
        'sql3':"AND A.CLM_NO IN ('00-300105')",
        'sql4':"AND A.CLM_NO IN ('00-300105')",
        }
    '''
    below test cases maynot work except 48
    '''
    senarios[42]={
        'sql1':"WHERE A.CLM_NO IN ('01-867930')",
        'sql2':"AND A.CLM_NO IN ('01-867930')",
        'sql3':"AND A.CLM_NO IN ('01-867930')",
        'sql4':"AND A.CLM_NO IN ('01-867930')",
        }

    senarios[43]={
        'sql1':"WHERE A.CLM_NO IN ('04-834131')",
        'sql2':"AND A.CLM_NO IN ('04-834131')",
        'sql3':"AND A.CLM_NO IN ('04-834131')",
        'sql4':"AND A.CLM_NO IN ('04-834131')",
        }

    senarios[44]={
        'sql1':"WHERE A.CLM_NO IN ('07-871347')",
        'sql2':"AND A.CLM_NO IN ('07-871347')",
        'sql3':"AND A.CLM_NO IN ('07-871347')",
        'sql4':"AND A.CLM_NO IN ('07-871347')",
        }

    senarios[45]={
        'sql1':"WHERE A.CLM_NO IN ('12-331303')",
        'sql2':"AND A.CLM_NO IN ('12-331303')",
        'sql3':"AND A.CLM_NO IN ('12-331303')",
        'sql4':"AND A.CLM_NO IN ('12-331303')",
        }

    senarios[46]={
        'sql1':"WHERE A.CLM_NO IN ('05-878010')",
        'sql2':"AND A.CLM_NO IN ('05-878010')",
        'sql3':"AND A.CLM_NO IN ('05-878010')",
        'sql4':"AND A.CLM_NO IN ('05-878010')",
        
        }
    senarios[47]={
        'sql1':"WHERE A.CLM_NO IN ('18-149612')",
        'sql2':"AND A.CLM_NO IN ('18-149612')",
        'sql3':"AND A.CLM_NO IN ('18-149612')",
        'sql4':"AND A.CLM_NO IN ('18-149612')",
        }

    senarios[48]={
        'sql1':"WHERE A.CLM_NO IN ('18-135381')",
        'sql2':"AND A.CLM_NO IN ('18-135381')",
        'sql3':"AND A.CLM_NO IN ('18-135381')",
        'sql4':"AND A.CLM_NO IN ('18-135381')",
        }

    return senarios


def get_test_table( logdir, senario, rows, where, db ):

#    fields=['PLCY_AGRE_ID','CLM_AGRE_ID','INS_PARTICIPANT','CLM_NO','PLCY_NO','PLCY_PRD_PTCP_EFF_DATE','PLCY_PRD_PTCP_END_DATE', 'CLM_PLCY_RLTNS_EFF_DATE','CLM_PLCY_RLTNS_END_DATE']
    fields =['PLCY_AGRE_ID', 'CLM_AGRE_ID', 'INS_PARTICIPANT', 'CLM_NO', 'PLCY_NO', 'CLM_PLCY_RLTNS_EFF_DATE', 'CLM_PLCY_RLTNS_END_DATE', 'CRNT_PLCY_IND']
    cols = ','.join( fields )


    where = where.replace( 'where C.agre_id IN', 'where CLM_AGRE_ID IN' )
    sql = 'select '+cols+' from {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY C ' + where
    senario_out = logdir+'senario_table_comparison_claim_policy_hist_'+str( senario )+'.txt'
    result = dblib.vsql2file( db['login'], db['passwd'], senario_out, sql, noheader=False, gzip=False, host=db[ 'server' ], delim="\t" )

    fw = open( senario_out, 'a+' )
    fw.write( '\n#-----TABLE COMPARISON\n' )
    for k,v in list( rows.items() ):
        for row in v:
            for col in fields:
                if col == 'INS_PARTICIPANT': col = 'CUST_ID'
                if col in row:
                    data = str(row[col]).replace(' 00:00:00','')
                    fw.write(data+'\t')
            fw.write('\n')
    fw.close()

def write_senario( senario, senario_dict, logdir, srcdb ):
    senario_finaldir = logdir+'final/'
    if not os.path.exists( senario_finaldir ):
        os.mkdir( senario_finaldir )

    senario_out = logdir+'senario_claim_policy_hist_'+str( senario )+'.txt'
    senario_final = logdir+'senario_claim_policy_hist_'+str( senario )+'.txt'
    print( 'writing Senario to', senario_out )
    print( 'place compare results in', senario_final )
    fw = open( senario_out, 'w' )

    for k,v in sorted( senario_dict.items() ):

        if 'items' in dir(v):
            fw.write('#------'+k+'\n')
            pprint.pprint(dict(v),fw)
        elif isinstance(v,list):
            fw.write('#------'+k+'\n')
            for row in v: print_row(row,fw)
        else:
            pprint.pprint( v, fw )
            print( senario, list( senario_dict.keys() ), v, srcdb )
            senarios = get_senarios()
            get_test_table( logdir, senario, senario_dict['final'], senarios[senario]['sql1'], srcdb )

    fw.close()

    for afile in sorted( os.listdir( senario_finaldir ) ):
        size1 = os.path.getsize( logdir+afile )
        size2 = os.path.getsize( senario_finaldir+afile )
        if size1 != size2:
            print( 'SCENARIO:', afile, 'Did not match!!!!!!!' )
        else:
            print( 'SCENARIO:', afile, 'MATCHED' )

def run_vsql_parallel( queries ):
    srcdb, sql, fname, args = queries
    run_vsql( srcdb, sql, fname, args )

def save_rows( fw, rows, col_order, delim = '\t' ):
    '''
    source rows:
    {'CLM_AGRE_ID': '8515456', 'INDST_CD': '11', 'INDST_CD_END_DT': datetime.datetime(2002, 6, 19, 0, 0),
    'CLM_NO': '00-300004', 'INDST_CD_EFF_DT': datetime.datetime(2000, 1, 2, 0, 0),
    'DW_UPDATE_DTTM': datetime.datetime(2017, 10, 31, 13, 14, 52, 972455),
    'DW_CREATE_DTTM': datetime.datetime(2017, 10, 31, 13, 14, 52, 972455),
    'CTL_ELEM_SUB_TYP_CD': '', 'INDST_NM': 'UNKNOWN', 'CRNT_IND': ''}

    ##target
    ##CREATE TABLE DW_REPORT.DW_CLAIM_MOD_INDUSTRY_HISTORY
    ##(
    ##    CLM_AGRE_ID numeric(31,0),
    ##    CLM_NO varchar(30),
    ##    INDST_CD varchar(15),
    ##    INDST_NM varchar(30),
    ##    INDST_CD_EFF_DT timestamp,
    ##    INDST_CD_END_DT timestamp,
    ##    CRNT_IND char(1),
    ##    DW_CREATE_DTTM timestamp,
    ##    DW_UPDATE_DTTM timestamp
    ##);
    ##   

    '''  
    



    
    for row in rows:
        #cleanup    
        #no cleanup
       
        #write rows      
        output_row = []
       
        if row[ 'INDST_CD_END_DT' ] == datetime.datetime(9999,12,31):
           row[ 'CRNT_IND' ] = 'y'
        else:
           row[ 'CRNT_IND' ] = 'n'
 
  
        
        if row[ 'INDST_CD_END_DT' ] < row[ 'INDST_CD_EFF_DT' ]:
            continue
        for col in col_order:
            output_row.append( str( row[col] ) )
            

           
           
        final_row = delim.join( output_row )

      
        final_row = final_row.replace( '9999-12-31 00:00:00','' )
        fw.write( final_row+'\n' )
        fw.flush()



def run( args, senario, senarios, data_dir, prog_name, srcdb, start_id = '', end_id = '', log_obj = None,):

    print(f'start_id = {start_id}, end_id = {end_id}')
    where1 = senarios[senario]['sql1']
    where2 = senarios[senario]['sql2']
    where3 = senarios[senario]['sql3']
    where4 = senarios[senario]['sql4']

    sql1 = get_sql( args, where1 )
    sql2 = get_sql2( args, where2 )        
    sql3 = get_sql3( args, where3 )
    sql4 = get_sql4( args, where4 )

    base_vsql_name = prog_name+'_'+str( senario )
    if start_id or end_id:
        if not start_id: start_id = 0
        if not end_id: end_id = 'end'
        base_vsql_name+='_'+str(start_id) + '_' + str(end_id)
    


    fname1 = os.path.join(data_dir,base_vsql_name+'_sql1.csv')
    fname2 = os.path.join(data_dir,base_vsql_name+'_sql2.csv')
    fname3 = os.path.join(data_dir,base_vsql_name+'_sql3.csv')
    fname4 = os.path.join(data_dir,base_vsql_name+'_sql4.csv')

    if False:
        queries=[
               ( srcdb, sql1, fname1, args ),
               ( srcdb, sql2, fname2, args ),
               ( srcdb, sql3, fname3, args ),
               ( srcdb, sql4, fname4, args ),
             ]

        #extract in parallel 4 sql queries
        s_results = inf.run_parallel( run_vsql_parallel, queries, len( queries ) )
    else:
        run_vsql( args, srcdb, sql1, fname1 )
        run_vsql( args, srcdb, sql2, fname2 )
        run_vsql( args, srcdb, sql3, fname3 )
        run_vsql( args, srcdb, sql4, fname4 )

    #make 4 file streams for each sql query
    #get_rows returns a named tuple for each row
    rows1 = get_rows( fname1 )
    rows2 = get_rows( fname2 )
    rows3 = get_rows( fname3 )
    rows4 = get_rows( fname4 )

    #using rows1 as a driver, process all 4 files

    base_fname = 'CLAIM_MOD_INDUSTRY'
    if start_id or end_id:
        if not start_id: start_id = 0
        if not end_id: end_id = 'end'
        base_fname+='_'+str(start_id)+'_'+str(end_id)
    final_fname = base_fname+'.csv'
    
    #fw = gzip.open( data_dir+final_fname, 'w', compresslevel=1 )
    with open(os.path.join( data_dir, final_fname), 'w') as fw:
        col_order = ( 'BWC_DW_EXTRACT_TS', 'BWC_DW_LOAD_KEY', 'CLM_AGRE_ID', 'CLM_NO', 'CRNT_IND', 'DW_CREATE_DTTM', 'DW_UPDATE_DTTM', 'INDST_CD', 'INDST_CD_EFF_DT', 'INDST_CD_END_DT', 'INDST_NM' )
        
        fw.write( '\t'.join( col_order )+'\n' )

        errors = []
  
        for rows in process_dbrows(args, rows1, rows2, rows3, rows4, errors, senario, args.logdir ):
            if rows:
                fixed_rows = []
                for row in rows:
                    row[f'BWC_DW_EXTRACT_TS'] = args.load_ts 
                    row[f'BWC_DW_LOAD_KEY'] = args.load_key 
                save_rows( fw, rows, col_order )
                #raw_input('go')
        # if errors:
        #     print( 'GOT ERRORS count:', len( errors ) )
        #     eprint = 'GOT ERRORS count:', len( errors )
        #     print( errors )
            #perrors = pprint.pformat( errors )
            #log_obj.info( eprint )
            #log_obj.info( 'Record batch that failed:' )
            #log_obj.info( perrors )
        
    print( 'finished', fw )
    #results_dict=process_dbrows(db_rows1_con,db_rows2_con,senario,log_obj)

    return final_fname

# def get_load_key(args, db, schema, load_type ):
#     Control_table = 'BWC_RPT.CNTL_DATES_BATCH_EL'
#     sql = '''select LOAD_KEY from %s where IS_MOST_RECENT_RUN = 'Y' and LOAD_TYPE='%s' '''%( Control_table,load_type )
#     #con = dblib.DB( db,log = 'info' )
#     con = get_dbcon( args, args.srcenv, args.srckey )

#     row = con.exe(sql).fetchone()
#     print(row)
#     load_key = row[ 'LOAD_KEY' ]
#     return load_key

def main_parallel(all_args):
    '''
       This assumes main has already created table to insert into
    '''
    args,parallel,scenario,args.data_dir,PROG_NAME,args.srcdb,min_agid,max_agid=all_args

    args.log=inf.setup_log(args.logdir,app=f'child_{args.temp_table}_{min_agid}')

    #dbsetup
    tgtdb_setup = dbsetup.Envs[args.tgtenv][args.tgtkey]
    srcdb_setup = dbsetup.Envs[args.srcenv][args.srckey]

    tgt_con = dblib.DB(tgtdb_setup, log=args.log, port = tgtdb_setup.get('port',''))
    # src_con = dblib.DB(srcdb_setup, log=args.log, port = srcdb_setup.get('port',''))
    # if args.warehouse:
    #     result=list(src_con.exe(f'USE WAREHOUSE {args.warehouse}'))[0]
    #     args.log.info(f'{result}')

    #now start processing

    args.log.info(f'processing {min_agid=},{max_agid=}')
    senarios=None

    final_fname = run( args, parallel, scenario, args.data_dir, PROG_NAME, args.srcdb, log_obj = args.log, start_id = min_agid, end_id = max_agid)

    args.log.info(f'loading MOD file into table DW_CLAIM_MOD_INDUSTRY_HISTORY')
    
    fq_file = args.data_dir/final_fname
    if not fq_file.exists():
        args.log.info(f'empty file = {fq_file}')
        return f'finished table  {args.temp_table} {min_agid}'

    tgt_con.load_file( args.tgtdb, args.tgtschema, args.temp_table, args.data_dir/final_fname, header=1, table_part=str(min_agid) )

    args.log.info(f'DW_CLAIM_MOD_INDUSTRY_HISTORY loaded')

    args.log.info('--- Writing data file')


    return f'finished table  {args.temp_table} {min_agid}'

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
    parser.add_argument( '--load_key', default='', required=False, )
    parser.add_argument( '--silent', default=False, action = 'store_true', required=False, )
    parser.add_argument( '--warehouse', default='', required=False, help='Snowflake Warehouse to use ' )
    parser.add_argument( '--cache', default = False, action = 'store_true', help = 'Cache results -- for re-runs? ' )
    parser.add_argument( '--debug', default=False, action='store_true', help='Toggle for debug messaging ' )
    parser.add_argument( '--dev', default=False, action='store_true', help='Changes ETL Dir from C to E if true' )
    parser.add_argument( '--min_agid', required = False, default = 0, help = 'min_agid - e.g. 1000000' )
    parser.add_argument( '--max_agid', required = False, default = 2000000000, help = 'max_agid - e.g. 6000000' )
    parser.add_argument( '--operation', required = False, default = 'policy_hist', help = 'Operation name to show.  Default: policy_hist' )
    parser.add_argument( '--table', required = False, default='DW_POLICY_INSRD_PARTICIPATION_HISTORY', help ='default table to output to: DW_POLICY_INSRD_PARTICIPATION_HISTORY' )
    parser.add_argument( '--parallel', required=False, default=12, help='number of parallel processes' )

    #python run_mod_ind_vsql.py  --operation mod_ind --srcenv dev_cognos  --srcdb vertica --s_schema DW_REPORT   --t_environment dev_cognos  --tgtdb vertica --t_schema DW_REPORT --load_type PCMP
    
    args = parser.parse_args()
    args.parallel = int( args.parallel )

    #eldir = f"C:/temp/{os.environ['USERNAME'].replace('_x', '')}/EL/" if args.dev else f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x', '')}/EL"

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    if args.dev: eldir=f"C:/TEMP/{os.environ['USERNAME'].replace('_x', '')}/EL"
    args.eldir = Path( eldir )
    args.prd = True if not args.dev else False
    args.root = Path(__file__).resolve().parent.parent
    #args.loaddir=args.root/'bwcpresent'

    #-- set the load key if not specified
    now = datetime.datetime.now()
    args.now = now
    ymd= now.strftime('%Y_%m_%d%p') #2021_05_14AM
    ymd_hms= now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key = ymd
    args.load_ts = ymd_hms

    # args.srcdir = args.srcdir.strip('/')
    # if args.srcdir.count('/') == 2:
    #     args.srcenv, args.srcdb, args.s_schema = args.srcdir.split('/')
    # else:
    #     args.s_schema = None
    #     args.srcenv, args.srcdb = args.srcdir.split('/', 1 )
    
    # args.tgtdir = args.tgtdir.strip('/')
    # if args.tgtdir.count('/') == 2:
    #     args.t_environment, args.tgtdb, args.t_schema = args.tgtdir.split('/')
    # else:
    #     args.t_schema = None
    #     args.t_environment, args.tgtdb = args.tgtdir.split('/', 1 )

    inf.build_args_paths( args, use_load_key = True )

    args.logdir=args.srclog
    
    args.data_dir = args.srcdata

    if not args.silent:
        args.log = inf.setup_log( args.logdir, app = 'parent' )
        args.log.info( f'processing in {args.eldir}' )
        print( f'\t=== Using ELDIR: {args.eldir}' )
        print( f'--Processing in: {args.eldir}' )

    args.table = 'DW_CLAIM_MOD_INDUSTRY_HISTORY'
    args.temp_table = 'DW_CLAIM_MOD_INDUSTRY_HISTORY_TEMP'
    
    return args

def err():
    import traceback, sys
    e='\t'+''.join( traceback.format_exception(*sys.exc_info()) )
    return e

def create_step1_table( args ):

    con1 = get_dbcon( args, args.tgtenv, args.tgtkey )

    sql=f'''
        create or replace TABLE {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP AS
        (
        select 
            CLM_AGRE_ID,
            CLM_NO,
            date_trunc( 'day', CLM_PLCY_RLTNS_EFF_DT ) as CLM_PLCY_RLTNS_EFF_DT, 
            case 
                when date_trunc('day',CLM_PLCY_RLTNS_END_DT) = lead(CLM_PLCY_RLTNS_EFF_DT) over (partition by CLM_AGRE_ID order by CLM_PLCY_RLTNS_EFF_DT, CLM_PLCY_RLTNS_END_DT)
                then DATEADD('DAY',-1,date_trunc('day',CLM_PLCY_RLTNS_END_DT))
                else CLM_PLCY_RLTNS_END_DT 
            end CLM_PLCY_RLTNS_END_DT, 
            CTL_ELEM_SUB_TYP_CD, 
            CRNT_PLCY_IND
        FROM 
            {args.tgtdb}.DW_REPORT.DW_CLAIM_POLICY_HISTORY)
    '''
    print(f'building {args.tgtdb}.PUBLIC.DW_CLAIM_POLICY_HISTORY_TEMP')
    con1.exe(sql)

def main():
    try:
        args = None
        args = process_args()

        ####OPTIONS#####
        PROG_NAME = os.path.basename( sys.argv[0] ).split('.')[0]

        if args.prd:
            run_senarios = []  #PROD
            args.cache = False  #PROD
        else:
            #run_senarios = range(1,20) #DEV ALL
            #run_senarios = range(1,5) #individual
            run_senarios = [1] #individual
            args.cache = False       #DEV

        create_step1_table( args )
        print(f'DW_CLAIM_POLICY_HISTORY_TEMP built')

        con1 = get_dbcon( args, args.srcenv, args.srckey )
        #con1.exe(f'truncate table {args.tgtdb}.{args.tgtschema}.{args.temp_table}')
        con1.exe(f'create or replace table {args.tgtdb}.{args.tgtschema}.{args.temp_table} like {args.tgtdb}.{args.tgtschema}.{args.table}')

        #load_key = get_load_key( args, args.srcdb, args.srcschema, load_type = 'PCMP' )

        #dir_info=inf2.get_dirs('dw_el',load_key,'PCMP',topdir=args.topdir)
        #root_dir=dir_info['root_dir']
        #log_obj=dir_info['log_obj']
        #root_dir=dir_info['run_dir']    
        #data_dir=dir_info['data_dir']   

        col_order = get_column_order()

        if True:
            args.log.info( f'Starting run:args.cache = { str( args.cache )} ' )
            #final_fname = run( args, 0, senarios, args.data_dir, PROG_NAME, args.srcdb, log_obj = args.log, start_id = args.min_agid, end_id = args.max_agid )

            all_args=[]
            for min_agid in range(0,10000000,1000000):
                max_agid = min_agid + 999999
                senarios = get_senarios( min_agid, max_agid )
                all_args.append([args,0,senarios,args.data_dir,PROG_NAME,args.srcdb,min_agid,max_agid])
            
            senarios = get_senarios( 10000000,20000000000 )
            all_args.append([args,0,senarios,args.data_dir,PROG_NAME,args.srcdb,10000000,20000000000])
            

            if len(all_args)==1 or args.parallel==1: 
                args.log.debug('Running in single threaded mode')
                results= [ main_parallel(allarg) for allarg in all_args ]
            else:
                results=inf.run_parallel(main_parallel,args=all_args,parallel=args.parallel,log=args.log)

            con1.exe(f'alter table {args.tgtdb}.{args.tgtschema}.{args.table} swap with {args.tgtdb}.{args.tgtschema}.{args.temp_table}')

            args.log.info( 'prd done' )

        else:
            data_dir = args.data_dir/PROG_NAME/'_senarios/'
            log_dir=args.logdir/PROG_NAME/'_senarios/'
            if not os.path.exists( data_dir ): os.makedirs( data_dir )
            if not os.path.exists( log_dir ): os.makedirs( log_dir )
            log_file=log_dir/os.path.basename(sys.argv[0])

            #log_obj=inf.getlog(log_file,stream=False)

            error_log=log_dir/'senario_errors.txt'
            fw=open(error_log,'w')
            for senario in run_senarios:
                #raw_input('senario:'+str(senario))
                try:
                    if senario in senarios:
                        #args.log.info('Starting run:args.cache='+str(args.cache))
                        print('Starting run:args.cache='+str(args.cache))
                        final_fname = run( args, senario, senarios, args.data_dir, PROG_NAME, args.srcdb, log_obj = args.log )
                except:
                    error=err()
                    print(error)
                    fw.write(time.asctime()+'\n'+error+'\n')
        

        args.log.info('done')

    except:
        if args:
            args.log.info(inf.geterr())
            raise 
        else:
            print(inf.geterr())
            raise

if __name__=='__main__':

    Debug=False

    main()
    
    '''
 python run_mod_ind_vsql.py  --operation mod_ind --srcenv dev_cognos  --srcdb vertica --s_schema DW_REPORT   --t_environment dev_cognos  --tgtdb vertica --t_schema DW_REPORT --load_type PCMP
 python run_mod_ind_vsql_snow.py  --operation mod_ind --srcdir prd/snow_etl/DW_REPORT --tgtdir prd/snow_etl/DW_REPORT --dev
 python run_mod_ind_vsql_snow.py  --operation mod_ind --srcdir /prd/snow_etl/RPD1/DW_REPORT --tgtdir /prd/snow_etl/RPD1/DW_REPORT --dev
 python run_mod_ind_vsql_snow.py  --operation mod_ind --srcdir /prd/snow_etl/RPD1/DW_REPORT --tgtdir /uat2/snow_etl/RUB1/DW_REPORT
 python run_mod_ind_vsql_snow.py  --operation mod_ind --srcdir /dev/snow_etl/RDA1/DW_REPORT --tgtdir /dev/snow_etl/RDA1/DW_REPORT --dev
    '''