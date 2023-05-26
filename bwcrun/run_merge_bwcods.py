#std libs
import sys,argparse,os
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
    libpath=str(prog_path.parent.parent.parent)
    sys.path.append(libpath)
    print('using path',libpath)

set_libpath()

from bwcenv.bwclib import dblib,inf,extract_info
from bwcsetup import dbsetup
from bwcsetup import setup_all
##added below to import setup_bwcods file
from bwcenv.bwcrun import setup_merge_bwcods


# ----------Main Program

# ----- custom sql functions in setup_merge 
def current_ind(tgt_col, end_date):
    '''
    For CAM data, end_date uses 2099-12-31 or 9999-12-31. If end_date is blank, current_ind defaults to 'Y'
    example sql below:
        (case when trim(APC_END_DATE) = '' then 'Y' when to_date(trim(APC_END_DATE),'YYYYMMDD') > current_date() then 'Y' else 'N' end) as APC_END_DATE
    '''
    generate_sql = f" (case when trim({end_date}) = '' then 'Y' when to_date(trim({end_date}),'YYYYMMDD') > current_date() then 'Y' else 'N' end) as {tgt_col} "
    return generate_sql

def derive_timestamp(src_col,tgt_col):
    '''
    Creates additional timestamp column from datetime source
    example sql below:
        to_timestamp(nullif(trim(RLTNS_BGN_DATE),''),'YYYYMMDDHHMISS') as RLTNS_BGN_TMSTM
    '''
    generate_sql = f" (case when trim({src_col}) = '' then '9999-12-31' else to_timestamp(nullif(trim({src_col}),''),'YYYYMMDDHHMISS') end) as {tgt_col} "
    return generate_sql

def add_date(tgt_col,starting_date,days_added):
    '''
    Adds specified number of days to source date
    example sql below:
        case when trim(MCO_RCPT_DATE)='' then null
		    else cast((to_timestamp(trim(MCO_RCPT_DATE),'YYYYMMDDHHMISS') + MCO_BWC_CDAYS_LAG ) as date) end as BWC_RCPT_DATE 
    '''
    generate_sql = f" case when trim({starting_date})='' then null else cast((to_timestamp(trim({starting_date}),'YYYYMMDDHHMISS') + {days_added} ) as date) end as {tgt_col} "
    return generate_sql

def srvc_lag(tgt_col,fdos,ldos):
    '''
    Adds specified number of days to source date
    example sql below:
        case when HSPTL_BILL_CODE in ('112','113') then datediff(day,CAST(FIRST_SRVC_DATE AS DATE),CAST(LAST_SRVC_DATE AS DATE))+ 1 
            else datediff(day,CAST(FIRST_SRVC_DATE AS DATE),CAST(LAST_SRVC_DATE AS DATE)) end as LNGTH_SRVC_LAG ,
    '''
    generate_sql = f" case when HSPTL_BILL_CODE in ('112','113') then datediff(day,CAST({fdos} AS DATE),CAST({ldos} AS DATE))+1 else datediff(day,CAST({fdos} AS DATE),CAST({ldos} AS DATE)) end as {tgt_col} "
    return generate_sql

def rename_column(src_col,tgt_col):
    '''
    Renames column as specified
    example sql below:
        trim(CARE_EOB_ID_NO) as PYMNT_BILL_NO
    '''
    generate_sql = f" trim({src_col}) as {tgt_col}  "
    return generate_sql

def cast_column(src_col,tgt_col):
    '''
    Renames column as specified
    example sql below:
        trim(CARE_EOB_ID_NO) as PYMNT_BILL_NO
    '''
    generate_sql = f" trim({src_col}) as {tgt_col}  "
    return generate_sql

def null_column(tgt_col):
    '''
    Hardcodes NULL for specified column
    example sql below:
        NULL as PYMNT_BILL_NO
    '''
    generate_sql = f" NULL as {tgt_col}  "
    return generate_sql

def hardcode_column(tgt_col,value):
    '''
    Hardcodes provided value for specified column
    example sql below:
        'CARE' as BILL_SRC_TYPE
    '''
    generate_sql = f" {value} as {tgt_col}  "
    return generate_sql

def high_date(end_date):
    '''
    Defaults blank end dates to high dates
    example sql below:
        (case when trim(RLTNS_END_DATE) = '' then '9999-12-31' else to_date(trim(RLTNS_END_DATE),'YYYYMMDD') end)  as RLTNS_END_DATE
    '''
    generate_sql = f" (case when trim({end_date}) = '' then '9999-12-31' else to_date(trim({end_date}),'YYYYMMDD') end) as {end_date} "
    return generate_sql

def amount_field(tgt_col):
    '''
    converts numeric values to amounts with two decimals
    example sql below:
        (to_number(nullif(trim(APC_AMT),''))/100) as APC_AMT 
    '''
    generate_sql = f" (to_number(nullif(trim({tgt_col}),''))/100) as {tgt_col} "
    return generate_sql

def substring_column(tgt_col,src_col,start,length,dtype):
    '''
    creates new column as a substring from source column. can specify substring location in parameters
    example sql below:
        substring(nullif(trim(CARE_EOB_ID_NO),''),1,2) as ACNTB_CODE 
    '''
    if not dtype=='':
        generate_sql = f" cast(substring(nullif(trim({src_col}),''),{start},{length}) as {dtype}) as {tgt_col} "
    else:
        generate_sql = f" substring(nullif(trim({src_col}),''),{start},{length}) as {tgt_col} "
    return generate_sql

def pivot(tgt_col,based_on,decode_column,decode_value,group_by):
    '''
    creates new column as a decode (case statement) from source column. Also generates necessary group by clause
    example sql below:
        max(case when SEQUENCE_NUMBER='01' then COMP_VALUE/100000 END) as DRG_TTL_AMT,
        max(case when SEQUENCE_NUMBER='02' then COMP_VALUE/100000 END) as LOS_AVG_DAYS_CNT,
        ... GROUP BY INVC_NO, DW_CNTRL_DATE, CARE_HDR_ID_NO
    '''
    generate_sql = f" max(case when {decode_column}='{decode_value}' then {based_on}/100000 END) as {tgt_col} "
    group_by_sql = f" GROUP BY {group_by} "
    return generate_sql,group_by_sql

def format_fin(tgt_col):
    '''
    formats Federal Tax ID number(FIN) like XX-XXXXX
    example sql below:
        substring(FDRL_TAX_ID,1,2)||'-'||substring(FDRL_TAX_ID,3) as FDRL_TAX_ID 
    '''
    generate_sql = f" substring(trim({tgt_col}),1,2)||'-'||substring(trim({tgt_col}),3,7) as {tgt_col} "
    return generate_sql

def format_phone_num(tgt_col):
    '''
    formats 7 digit phone number like XXX-XXXX
    example sql below:
        substring(nullif(trim(SRVC_PHONE_NO),''),1,3)||'-'||substring(nullif(trim(SRVC_PHONE_NO),''),4) as SRVC_PHONE_NO 
    '''
    generate_sql = f" substring(nullif(trim({tgt_col}),''),1,3)||'-'||substring(nullif(trim({tgt_col}),''),4) as {tgt_col} "
    return generate_sql

def format_emp_id(tgt_col):
    '''
    If employee ID is an A#, remove 'A' from ID
    example sql below:
        case when REGEXP_LIKE(trim(upper(UPDT_EMPLY_ID_NO)),'A[0-9]+') then substring(trim(cent.CRT_USER_CODE),2)
	    else trim(cent.CRT_USER_CODE) END as CRTFC_USER_CODE
    '''
    generate_sql = f" case when REGEXP_LIKE(trim(upper({tgt_col})),'A[0-9]+') then substring(trim({tgt_col}),2) else trim({tgt_col}) END as {tgt_col} "
    return generate_sql

def lookup(lookup_table,lookup_column,source_table,source_column,target_column,join_column,filter):
    '''
    uses external table to lookup required value
    example sql below:
        left join (
		        select PRVDR_TYPE_CODE as lkup_key,
		        PRVDR_TYPE as lkup_val
	    from BWCODS.TCDPRTH 
        WHERE CRNT_CODE_FLAG = 'Y') BWCODSTCDPRTH_lkup1 on nullif(trim(BWC_ETL.BWCSTAGE_DW_INVOICE_HEADER.PAYTO_PRVDR_CODE),'') = nullif(trim(BWCODSTCDPRTH_lkup1.lkup_key),'')
    '''
    if not filter =='':
        src_sql = f'(select {join_column} as lkup_key, {source_column} as lkup_val from {lookup_table} WHERE {filter}) {lookup_table.replace(".","")}_{target_column}'
    else:
        src_sql = f'(select {join_column} as lkup_key, {source_column} as lkup_val from {lookup_table}) {lookup_table.replace(".","")}_{target_column}'

    join_condition = f''' left join {src_sql} on nullif(trim({source_table}.{lookup_column}),'') = nullif(trim({lookup_table.replace(".","")}_{target_column}.lkup_key),'') '''
    
    generate_sql = f' trim({lookup_table.replace(".","")}_{target_column}.lkup_val) as {target_column} '
    #print(f'       ----------------------->{join_condition}')
    return join_condition, generate_sql

# -----Functions to construct insert statements from CAM files into BWCODS
def build_custom_sql(args,srctable,tgt_col,tgt_column_dict,group_by):
    group_by_sql=join_condition=generate_sql=''
    operation = tgt_column_dict.get('operation')
    if operation == 'current_ind':
        end_date_col = tgt_column_dict['based_on']
        generate_sql = current_ind(tgt_col, end_date_col)
    elif operation == 'derive_timestamp':
        src_col = tgt_column_dict['based_on']
        generate_sql = derive_timestamp(src_col,tgt_col)
    elif operation == 'rename_column':
        src_col = tgt_column_dict['based_on']
        generate_sql = rename_column(src_col,tgt_col)
    elif operation == 'null_column':
        generate_sql = null_column(tgt_col)
    elif operation == 'hardcode_column':
        value = tgt_column_dict['value']
        generate_sql = hardcode_column(tgt_col,value)
    elif operation == 'amount_field':
        generate_sql = amount_field(tgt_col)
    elif operation == 'substring_column':
        src_col = tgt_column_dict['based_on']
        start = tgt_column_dict['start']
        length = tgt_column_dict['length']
        dtype = tgt_column_dict['dtype']
        generate_sql = substring_column(tgt_col,src_col,start,length,dtype)
    elif operation == 'pivot':
        based_on = tgt_column_dict['based_on']
        decode_column = tgt_column_dict['decode_column']
        decode_value = tgt_column_dict['decode_value']
        generate_sql, group_by_sql = pivot(tgt_col,based_on,decode_column,decode_value,group_by)
    elif operation == 'lookup':
        lookup_table = tgt_column_dict['based_on_table']
        lookup_column = tgt_column_dict['based_on_column']
        source_table=f'{args.srcschema}.{srctable}'
        source_column = tgt_column_dict['source_column']
        join_column = tgt_column_dict['join_column']
        filter = tgt_column_dict['filter']
        join_condition, generate_sql = lookup(lookup_table, lookup_column, source_table,source_column,tgt_col,join_column,filter)
    elif operation == 'high_date':
        generate_sql = high_date(tgt_col)
    elif operation == 'format_fin':
        generate_sql = format_fin(tgt_col)
    elif operation == 'format_phone_num':
        generate_sql = format_phone_num(tgt_col)
    elif operation == 'format_emp_id':
        generate_sql = format_emp_id(tgt_col)
    elif operation =='add_date':
        starting_date = tgt_column_dict['starting_date']
        days_added = tgt_column_dict['days_added']
        generate_sql = add_date(tgt_col,starting_date,days_added)
    elif operation == 'srvc_lag':
        fdos = tgt_column_dict['fdos']
        ldos = tgt_column_dict['ldos']
        generate_sql = srvc_lag(tgt_col,fdos,ldos)
    elif operation == 'to_do':
        pass             
    elif not operation:
        pass
    else: raise ValueError(f'Process not supported {operation}')
    return group_by_sql,join_condition, generate_sql

def convert_varchar(args,tgt_col,dtype):
    if dtype == 'TIMESTAMP':
                generate_sql = f" to_timestamp(nullif(trim({tgt_col}),''),'YYYYMMDDHHMISS') as {tgt_col} "
    elif dtype == 'DATE':
        generate_sql = f" to_date(nullif(trim({tgt_col}),''),'YYYYMMDD') as {tgt_col} "
    elif dtype.startswith('NUMERIC') or dtype.startswith('INT'):
        generate_sql = f"  to_number(nullif(trim({tgt_col}),'')) as {tgt_col} "
    else:
        generate_sql = f" cast( trim({tgt_col}) as {dtype} ) as {tgt_col} "
    return generate_sql

def create_insert(args, srctable, tgttable, custom_columns_dict,group_by):
    '''
    INSERT INTO a82581.TCDAPCI(APC_CODE, APC_BGN_DATE, APC_END_DATE, APC_DESC, APC_STS_IND_CODE, APC_STS_IND_DESC, APC_AMT, APCI_CRNT_FLAG, DW_CNTRL_DATE) 
    select APC_CODE, to_date(trim(APC_BGN_DATE),'yyyymmdd'), to_date(trim(APC_END_DATE),'yyyymmdd'), APC_DESC, APC_STS_IND_CODE, APC_STS_IND_DESC, to_number(nullif(trim(APC_AMT),'')) as APC_AMT, 'Y' as APCI_CRNT_FLAG, to_date(trim(DW_CNTRL_DATE),'yyyymmdd') 
    from A82581.bwcstage_DW_APC_INFO;
    '''
    #tgttable=srctable.upper().replace(setup_merge_bwcods.staged_file_prefix.upper()+'_','')
    
    where_clause = f''
    join_condition=f''
    group_by_sql=f''
    tgt_ddl_dict = args.con.ddl2dict(args.tgtschema,tgttable)
    src_ddl_dict = args.con.ddl2dict(args.srcschema,srctable)
    if 'CHANGE_INDICATOR' in src_ddl_dict:
        where_clause = f"WHERE CHANGE_INDICATOR <> 'T'"
        CI_FLAG=True
    
    insert_sql = f'INSERT INTO {args.tgtschema}.{tgttable} ('
    
    unused_tgt_cols = set(tgt_ddl_dict)-set(src_ddl_dict)
    
    tgt_cols = ', '.join(list(tgt_ddl_dict))
    src_cols = ', '.join(list(src_ddl_dict))
    insert_sql += tgt_cols+')\n'
    #
    generate_sql_list = []
    master_join=''  
    master_group=''
    for tgt_col,dtype in tgt_ddl_dict.items():
        generate_sql = ''  
        if tgt_col in custom_columns_dict:
            #current_column_dict={'operation':'current_ind','based_on':'APC_END_DATE',},
            tgt_column_dict = custom_columns_dict[tgt_col]
            group_by_sql,join_condition,generate_sql=build_custom_sql(args,srctable,tgt_col,tgt_column_dict,group_by)
            #if not master_join: master_join=join_condition
            master_join+=join_condition
            #else:
                #if join_condition: raise ValueError(f'Found extra join {join_condition} for {srctable},{tgttable}')
        elif tgt_col not in unused_tgt_cols:
            #standard conversion of CAM file varchars to desired datatypes
            generate_sql=convert_varchar(args,tgt_col,dtype)
        else: raise ValueError(f'Unsupported target column {tgt_col}')

        generate_sql_list.append(generate_sql)

    cast_cols = 'select '+', '.join(generate_sql_list)
    insert_sql += cast_cols
    sql_from = f' FROM {args.srcschema}.{srctable} \n{master_join} {where_clause} {group_by_sql}'
    #
    insert_sql += sql_from
    
    return insert_sql, CI_FLAG

# ----- 
def validate(args, srctable, tgttable, tgtstrategy, CI_FLAG):
    '''
    Row counts will not match if it has CHANGE_INDICATOR( One row less compared to source table)
    '''
    error=''
    if tgtstrategy == 'bulk':
        tgt_count = args.con.row_count(args.tgtschema,tgttable)
        src_count = args.con.row_count(args.srcschema,srctable) - CI_FLAG
        if tgt_count != src_count:
            error = f'Count mismatch {args.tgtschema}.{tgttable}: {tgt_count} vs {args.srcschema}.{srctable}: {src_count}\n'
        else: print(f'Counts between {args.srcschema}.{srctable} and {args.tgtschema}.{tgttable} match: {tgt_count}\n')
        tgt_keys = args.con.get_keys(args.tgtschema,tgttable)
    return error

def run_load_strategy(args,tgttable,tgtstrategy):
    if tgtstrategy == 'bulk':
            truncate_sql = f' TRUNCATE TABLE {args.tgtschema}.{tgttable};'
            args.con.exe(truncate_sql)
    # elif tgtstrategy == 'incr':
    #         truncate_sql = f'delete from {args.tgtschema}.{tgttable} where ({pk}) in (select {pk} from {args.srcschema}.{srctable})'
    #         args.con.exe(truncate_sql)
    elif tgtstrategy=='delete_insert':
        pass #delete
    else: raise ValueError(f'Target strategy {tgtstrategy} is not supported')

# src_primary_keys=srccon.get_keys(args.srcschema,table)
# src_col_names=srccon.get_cols(args.srcschema,table)
# pk=(','.join(src_primary_keys))
# cols=(','.join(src_col_names))

def run_insert(args,tgtstrategy,tgttable,tgt_columns_dict,srctable,group_by):
    tgtsql,CI_FLAG = create_insert(args, srctable, tgttable, tgt_columns_dict,group_by)
    args.con.exe(tgtsql)

    error_msg = validate(args, srctable, tgttable, tgtstrategy, CI_FLAG)
    return error_msg

def process_error(args, error_msg):
    args.log.info(f'Found Errors- {error_msg}')

# -----standard startup functions
def process_args():
    '''
    Process given arguments 
    '''

    #eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir =setup_all.eldir
    parser = argparse.ArgumentParser(description='Command line args',epilog='hello again')
    #required
    parser.add_argument('--srcdir',required=True,help='directory for source data to be compared')
    parser.add_argument('--tgtdir',required=True,help='directory for target data to be merged ')
    #boolean
    #optional
    parser.add_argument('--eldir', default=eldir,help='default directory for all logs, data files')
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )
    parser.add_argument('--sql',required=False, help='sql to be executed')
    #
    args= parser.parse_args()

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srcdata
    #use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args, use_load_key=False, find_src_load_key=False)

    args.logdir = args.tgtlog
    args.log = inf.setup_log(args.logdir, app='parent', cleanlog=args.cleanlog)
    args.log.info(f'processing in {args.eldir}')

    return args

def main():

    args=process_args()
    if args == None: return 1

    srcdb = dbsetup.Envs[args.srcenv][args.srckey]
       
    args.con=dblib.DB(srcdb,log=args.log,port=srcdb.get('port',''))
    prefix=setup_merge_bwcods.staged_file_prefix.upper()+'_'    
    cam_dbfiles_to_convert=set([table for table in args.con.get_tables(args.srcschema) if table.startswith(prefix)])
    args.log.info(f'{cam_dbfiles_to_convert}')
    
    for camfile in cam_dbfiles_to_convert:
        grouping=''
        CI_FLAG = False
        camfile = camfile.upper()
        #staged_files['BWCSTAGE_DW_APC_INFO']={target,strategy,columns}
        if camfile not in setup_merge_bwcods.staged_files: continue
        ## Will eventually change code to throw an error if not defined in setp_merge_bwcods.staged_files
        tgttable = setup_merge_bwcods.staged_files[camfile]['target']
        tgtstrategy = setup_merge_bwcods.staged_files[camfile]['strategy']
        custom_columns_dict = setup_merge_bwcods.staged_files[camfile].get('columns','')
        grouping = setup_merge_bwcods.staged_files[camfile].get('group_by','')
        if not grouping == '':
            group_by = ', '.join(grouping)
        else: group_by = ''
 
        if 'to_do' in tgtstrategy: continue        
        run_load_strategy(args,tgttable,tgtstrategy)
        error_msg=run_insert(args,tgtstrategy,tgttable,custom_columns_dict,camfile,group_by)
        #break

        if error_msg:
            process_error(args, error_msg) 

    print(f'----------DONE----------')
        
        
### python run_merge_bwcods.py  --srcdir /dev/vertica_etl/RUB1/BWC_ETL --tgtdir /dev/vertica_etl/RUB1/BWCODS_NEW   

if __name__=='__main__':
    main()

'''
        to_process=[              
                    'DW_APC_INFO',
                    'DW_API_SUM',
                    'DW_DIAG',
                    'DW_DRG_CODE',
                    'DW_EDT',
                    'DW_EOB',
                    'DW_HDR_COND',
                    'DW_HDR_HSPTL_PROC',
                    'DW_HDR_OCCSPAN',
                    'DW_HDR_VALUE',
                    'DW_ICD_PROC_CODE',
                    'DW_INV_DRG_COMP',
                    'DW_INVOICE_EDTEOB',
                    'DW_INVOICE_HEADER',
                    'DW_INVOICE_LINE',
                    'DW_INVOICE_LINE_APC',
                    'DW_INVOICE_LINE_DIST',
                    'DW_INVOICE_LINE_MOD',
                    'DW_INVOICE_TCN',
                    'DW_NETWORK',
                    'DW_NETWORK_COUNTY',
                    'DW_NETWORK_PAYMENTS',
                    'DW_POLICY',
                    'DW_POLICY_NETWORK',
                    'DW_REF',
                    'DW_REV_CPT_CODE',
                     ]

        to_process=[f'{prefix}{atable}' for atable in to_process]

        #print(to_process)

        args.log.info(str(to_process))
        if srctable not in to_process: 
            args.log.info(f'skipping {srctable}')
            continue

'''