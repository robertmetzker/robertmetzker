'''
Changed 2022-08-04 (A85275) to do a DIRECT insert into Oracle.  This removes the movement of data across the network and keeps all processing in Oracle.
This change allowed 1.7 million records to insert in 8 seconds vs. 70k over the course of 2.5 hours.
In addition, the TO_TIMESTAMP was converted to just be DATE (as DATE has timestamp) as the Timestamp was not capturing the correct info.
'''
#std libs
import sys, argparse, os, datetime
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
    prog_path = Path(os.path.abspath(__file__))
    root = prog_path.parent.parent.parent
    pyversion = f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup


def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    parser = argparse.ArgumentParser(description='command line args', epilog="Example:python extract.py --srcdir /dev/cam/base", add_help = True)
    
    #required
    parser.add_argument( '--srcdir', required = True, help = '/env/database/schema/[table]')

    #boolean
    # parser.add_argument( '--limit_rows',default=False,action='store_true',help='Set row limit, default=10')

    #optional
    parser.add_argument( '--eldir', default = eldir, help = 'default directory to dump the files')
    parser.add_argument( '--level', default = 1, help = 'where in call chain this is for logging')
    parser.add_argument( '--load_key', default = '', help = 'load_key to use (defaults to current date as YYYY_MM_DD')
    parser.add_argument( '--logdir', default = '', help = 'default logging directory, $root/env/conn/schema/load_key/logs')
    parser.add_argument( '--table', help = 'specific table to extract when not doing full_schema, e.g. DATE_DIM')

    args = parser.parse_args()


    #-- set the load key if not specified
    now = datetime.datetime.now()
    args.start = now
    ymd = now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms = now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');

    #alternative way to determine load file location
    inf.build_args_paths(args, use_load_key = False, find_src_load_key = False)
    args.prefix=''

    args.root=Path(__file__).resolve().parent.parent

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir=args.srclog

 # Directories will be created if they do not already exist.
    args.log = inf.setup_log(args.logdir, app='parent', prefix=args.level*'\t')
    args.log.debug(f'INFO: args global settings:{args}')

    return args

def get_prev2_weeks( srccon ):
    # Calculate Sunday 2 weeks ago by going back 3 weeks and using NEXT_DAY function.  Truncate to get midnight.  Add 0.99999 to get 23:59:59
    oracle_sql = "SELECT SYSDATE-14 as cdc_bgng_dttm, SYSDATE as cdc_endng_dttm FROM DUAL"
    prev2_weeks = srccon.fetchone( oracle_sql )
    
    return prev2_weeks


def get_change_tracking_data( args, srccon, p_pcmp_schema, p_etl_schema, p_cdc_bgng_dttm, p_cdc_endng_dttm ):
    '''
    get data from Change Tracking tables in the Oracle database

    sample data from query below:
    BI_CHG_TRK_ID  MDCT_TBL_NM      MDCT_PK_COL_NM  CHG_TRK_PK_ID  Z_ETL_PRCS_DTM         
                1  ACTIVITY_DETAIL  ACTV_DTL_ID        1000000728  09/22/2016 7:33:06 AM  
                2  ACTIVITY_DETAIL  ACTV_DTL_ID        1000000730  09/22/2016 7:33:06 AM  
                3  ACTIVITY_DETAIL  ACTV_DTL_ID        1000000729  09/22/2016 7:33:06 AM  
    '''
    # {'CMN_CHG_TRK_ID': 1237956002, 'MDCT_ID': 32, 'CMN_CHG_TRK_PK_ID': 1325604109, 'CMN_CHG_TRK_UPDT_IND': 'n', 'CMN_CHG_TRK_ROLLUP_IND': 'y', 'AUDIT_USER_ID': 1, 'AUDIT_USER_CREA_DTM': datetime.datetime(2019, 1, 2, 10, 53, 54, 661254)}

    pcmp_table1 = p_pcmp_schema + '.common_change_tracking'
    pcmp_table2 = p_pcmp_schema + '.meta_data_change_tracking_cd'
    pcmp_table3 = p_pcmp_schema + '.claim_change_tracking'
    # etl_table   = p_etl_schema  + '.bwc_bi_change_tracking'
    etl_table   = p_etl_schema  + '.bwc_bi_change_tracking_sf'

    print( f"----Source Tables\n----\npcmp_table1: {pcmp_table1}\npcmp_table2: {pcmp_table2}\npcmp_table3: {pcmp_table3}\netl_table: {etl_table}\n----" )
    # args.log.info( f"----Source Tables\n----\npcmp_table1: {pcmp_table1}\npcmp_table2: {pcmp_table2}\npcmp_table3: {pcmp_table3}\netl_table: {etl_table}\n----")

    sql = f'''
    INSERT INTO {etl_table} ( bi_chg_trk_id, mdct_tbl_nm, pk_col_nm, chg_trk_pk_id, z_etl_prcs_dtm ) 
SELECT 
       ROW_NUMBER () OVER ( PARTITION BY mid.MAX_ID  order by  mid.MAX_ID )     as BI_CHG_TRK_ID, 
       a.mdct_src_tbl_nm                                                        as MDCT_TBL_NM,
       a.mdct_src_tbl_pk_col_nm                                                 as MDCT_PK_COL_NM, 
       a.cmn_chg_trk_pk_id                                                      as CHG_TRK_PK_ID, 
       CURRENT_DATE                                                             as Z_ETL_PRCS_DTM
  FROM (
        SELECT DISTINCT cct.cmn_chg_trk_pk_id       AS CMN_CHG_TRK_PK_ID,
                        mdctc.mdct_src_tbl_nm       AS MDCT_SRC_TBL_NM,
                        mdct_src_tbl_pk_col_nm      AS MDCT_SRC_TBL_PK_COL_NM,
                        CURRENT_DATE                AS Z_ETL_PRCS_DTM
          FROM {pcmp_table1} cct
    INNER JOIN {pcmp_table2} mdctc ON
               ( mdctc.mdct_id = cct.mdct_id )
           AND mdctc.end_dtm        IS NULL
         WHERE mdct_src_tbl_nm IN   ('PARTICIPATION','NOTE_APPLICATION_DETAIL_LEVEL','CALCULATION_RESULT','ACTIVITY_DETAIL')
           AND audit_user_crea_dtm >= TO_DATE('{p_cdc_bgng_dttm}', 'YYYY-MM-DD HH24:MI:SS')
           AND audit_user_crea_dtm <= TO_DATE('{p_cdc_endng_dttm}', 'YYYY-MM-DD HH24:MI:SS')
UNION ALL 
        SELECT DISTINCT cct.clm_chg_trk_pk_id       AS CMN_CHG_TRK_PK_ID,
                        mdctc.mdct_src_tbl_nm       AS MDCT_SRC_TBL_NM,
                        mdct_src_tbl_pk_col_nm      AS MDCT_SRC_TBL_PK_COL_NM,
                        CURRENT_DATE                AS Z_ETL_PRCS_DTM  
          FROM {pcmp_table3} cct
    INNER JOIN {pcmp_table2} mdctc ON 
               ( mdctc.mdct_id = cct.mdct_id )
           AND mdctc.end_dtm        IS NULL
         WHERE mdct_src_tbl_nm IN   ('PARTICIPATION','NOTE_APPLICATION_DETAIL_LEVEL','CALCULATION_RESULT','ACTIVITY_DETAIL')
           AND audit_user_crea_dtm >= TO_DATE('{p_cdc_bgng_dttm}', 'YYYY-MM-DD HH24:MI:SS')
           AND audit_user_crea_dtm <= TO_DATE('{p_cdc_endng_dttm}', 'YYYY-MM-DD HH24:MI:SS')
       ) a
  INNER JOIN ( SELECT COALESCE( MAX( bi_chg_trk_id ),0 ) AS max_id FROM {etl_table} ) mid ON 1=1
         '''

    print( f'Executing sql: {sql}' )
    # args.log.info( f'Executing sql: {sql}')
    
    row_cnt = 0

    result = srccon.exe(sql, commit=True )

    # for row in srccon.fetchall(sql):
    #     row_cnt = row_cnt + 1
    #     bi_chg_trk_id, mdct_tbl_nm, mdct_pk_col_nm, chg_trk_pk_id, z_etl_prcs_dtm = row
    #     # print(f'\t{bi_chg_trk_id}, {mdct_tbl_nm}, {mdct_pk_col_nm}, {chg_trk_pk_id}, {z_etl_prcs_dtm}')
    #     # 7883, ACTIVITY_DETAIL, ACTV_DTL_ID, 1752729437, 2022-07-14 08:46:14

    #     add_change_tracking_data( srccon, etl_table, bi_chg_trk_id, mdct_tbl_nm, mdct_pk_col_nm, chg_trk_pk_id, z_etl_prcs_dtm )

    # print ( f'--- Inserted {row_cnt} rows')
    changes = srccon.exe(f'select count(*) as rows_inserted from {etl_table}', commit=False )
    for row in changes:
        print ( f'--- Insertion of {row[0]} changes has completed.')

def add_change_tracking_data( srccon, etl_table, bi_chg_trk_id, mdct_tbl_nm, mdct_pk_col_nm, chg_trk_pk_id, z_etl_prcs_dtm):
    '''
    create a new row in bwc_bi_change_tracking table in Oracle database
    '''

    # INSERT statement with dictionaries does not work in Oracle (works in Vertica via "exem" method);
    # therefore, not using dictionaries here and also using a different ("exe") method as well
        # SELECT * FROM all_tab_columns WHERE table_name = 'BWC_BI_CHANGE_TRACKING';  --BI_CHG_TRK_ID not nullable/PK?
    sql = f"INSERT INTO {etl_table} ( bi_chg_trk_id, mdct_tbl_nm, pk_col_nm, chg_trk_pk_id, z_etl_prcs_dtm ) VALUES( '{bi_chg_trk_id}', '{mdct_tbl_nm}', '{mdct_pk_col_nm}', {chg_trk_pk_id}, TO_TIMESTAMP('{z_etl_prcs_dtm}', 'YYYY-MM-DD HH24:MI:SS.FF') )"
    result = srccon.exe(sql, commit=False)

    # Verify the inserts via the following SQL:
    # Need to correct:  cx_Oracle.IntegrityError: ORA-00001: unique constraint (BWC_ETL.XPK_BI_CT) violated
    # SELECT cc.*
    #   FROM all_constraints c
    #   JOIN all_cons_columns cc ON (c.owner = cc.owner
    #    AND c.constraint_name = cc.constraint_name)
    #  WHERE c.table_name = 'BWC_BI_CHANGE_TRACKING';

    # BWC_ETL	XPK_BI_CT	    BWC_BI_CHANGE_TRACKING	BI_CHG_TRK_ID	1
    # BWC_ETL	SYS_C001111280	BWC_BI_CHANGE_TRACKING	BI_CHG_TRK_ID	

    # SELECT * FROM bwc_etl.BWC_BI_CHANGE_TRACKING   WHERE z_etl_prcs_dtm >= trunc(sysdate-21);

    
def main():
    '''
    for BI Change Tracking, get data from source table and copy them to target table

    to execute program, enter the following on the command line:
    python tmp_bi_changes_tracking.py --srcdir /dev/oracle_etl/DB/PCMP 
    '''
    args=process_args()

    print('==== Job started at ' + str(args.start)[0:19]) # format is 'YYYY-MM-DD HH:MM:SS.SSSSSS'
    print('Command line arguments:',str(args))

    try:
        # print(dbsetup.Envs[args.srcenv])
        # args.srcenv, args.srckey, args.srcdb, args.srcschema=args.srcdir.split('/')
        # 'oracle_etl': {'server': 'bwcsxicsorda01', 'db': 'pda1', 'login': 'BWC_ETL', 'passwd': '==', 'type': 'oracle'}
        srcdb = dbsetup.Envs[args.srcenv][args.srckey]
        # {'server': 'bwcsxicsorda01.testbwcad.testad.bwc.state.oh.us', 'db': 'pda1', 'login': 'BWC_ETL', 'passwd': 'YzAwbGJyZWV6ZQ==', 'type': 'oracle'}

        srccon = dblib.DB(srcdb, log=args.log, port = srcdb.get('port',''))
        prev2_sunday, prev_saturday = get_prev2_weeks( srccon )
        print( f'Previous Sunday Date: {prev2_sunday} through {prev_saturday}')
        # Previous Sunday Date: 2022-06-26 00:00:00 through 2022-07-09 23:59:59
        # cdc_bgng_dttm = prev2_sunday
        # cdc_endng_dttm = prev_saturday

        #truncate the BWC_ETL table
        # sql_truncate = 'truncate TABLE BWC_ETL.bwc_bi_change_tracking'
        sql_truncate = 'truncate TABLE BWC_ETL.bwc_bi_change_tracking_sf'
        srccon.exe( sql_truncate, commit = True ) 
        print ('==== BWC_BI_CHANGE_TRACKING_SF table truncated...: ', sql_truncate)

        get_change_tracking_data(args, srccon, args.srcschema, 'BWC_ETL', prev2_sunday, prev_saturday )
        # Includes add_change_tracking_data to write to the oracle BWC_ETL table:  BWC_BI_CHANGE_TRACKING
            
        srccon.commit()
        print ('==== Committed changes')

        # {'CMN_CHG_TRK_ID': 1237956002, 'MDCT_ID': 32, 'CMN_CHG_TRK_PK_ID': 1325604109, 'CMN_CHG_TRK_UPDT_IND': 'n', 'CMN_CHG_TRK_ROLLUP_IND': 'y', 'AUDIT_USER_ID': 1, 'AUDIT_USER_CREA_DTM': datetime.datetime(2019, 1, 2, 10, 53, 54, 661254)}
        # {'CMN_CHG_TRK_ID': 1237956003, 'MDCT_ID': 32, 'CMN_CHG_TRK_PK_ID': 1325604110, 'CMN_CHG_TRK_UPDT_IND': 'n', 'CMN_CHG_TRK_ROLLUP_IND': 'y', 'AUDIT_USER_ID': 1, 'AUDIT_USER_CREA_DTM': datetime.datetime(2019, 1, 2, 10, 53, 54, 662588)}

        args.log.info(f'---=# Process is Completed #=---')
    except:
        error = inf.geterr()
        print(f'\n!!! EXCEPTION ERROR: {error}')
        args.log.error(error)
        sys.exit(1)

    args.end = datetime.datetime.now()
    print( '==== Job ended at ' + str( args.end )[0:19] ) # format is 'YYYY-MM-DD HH:MM:SS.SSSSSS'


if __name__=='__main__':
    main()

# Example execution:
#     python run_bi_change_tracking.py --srcdir /dev/oracle_etl/DB/PCMP
#     python run_bi_change_tracking_direct.py --srcdir /uat/oracle_etl/pub1/PCMP

#     # ACTIVITY_DETAIL - uses BWC_BI_CHANGE_TRACKING table
#     # NOTE_APPLICATION_DETAIL_LEVEL - uses BWC_BI_CHANGE_TRACKING table
#     # PARTICIPATION - uses BWC_BI_CHANGE_TRACKING table

  