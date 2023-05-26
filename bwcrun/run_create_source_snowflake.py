import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
from pathlib import Path


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

def get_table_cols(args,tables,dbcon,db):
    '''
    Note: This function makes best effort to get column tables, if table does not exist there will not be any columns.
    db is a optional arugument because in snowflake the views can be created in another database.
    
    {'ACCOUNT_TYPE': ['ACCT_TYP_CD', 'ACCT_TYP_NM', 'ACCT_TYP_VOID_IND', 'BWC_DW_EXTRACT_TS', 'BWC_DW_LOAD_KEY', 'BWC_DW_LOAD_START_DTTM'],
     'BWCSTAGEDW_APC_INFO': ['APC_AMT', 'APC_BGN_DATE', 'APC_CODE', 'APC_DESC', 'APC_END_DATE', 'APC_STS_IND_CODE', 'APC_STS_IND_DESC', 'BWC_DW_LOAD_START_DTTM', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE'], 
     'BWCSTAGEDW_API_SUM': ['API_RCPT_DATE', 'BWC_DW_LOAD_START_DTTM', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE', 'MCO_ID_NO', 'MCO_NAME', 'NTWRK_ID_NO', 'TOTAL_FAIL_QTY', 'TOTAL_PASS_QTY', 'TOTAL_SBMTD_QTY'], 
     'BWCSTAGEDW_DIAG': ['BWC_DW_LOAD_START_DTTM', 'CARE_HDR_ID_NO', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE', 'ICD_CODE', 'ICD_SQNC_NO', 'INVC_NO', 'PRNCP_ICD_FLAG', 'PRSNT_ADMSN_TEXT']}
    '''
    table_cols_dict={}
    table_cols=dbcon.get_cols(args.srcschema,db=db)

    for idx,table_dict in enumerate(tables):
        #atable=table_dict['table']
        atable=table_dict.get('table')
        if not atable: 
            args.log.info(f'Bad Row {table_dict}')
            continue
        #tcols=table_cols[atable]
        tcols=table_cols.get(atable)
        #if not tcols:raise ValueError(f'Missing Table in Snowflake {atable}')
        table_cols_dict[atable]=tcols
        
    return table_cols_dict


def get_load_version_dict(args):
    args.log.info(f'Using load file {args.load_file}')
    source_rows=[]
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['table'].startswith('#'): continue
        source_rows.append(source_dict)

    return source_rows


def check_pks(args,load_version_tables,src_table_cols_dict):
    key_delim="||'-'||"
    all_missing_keys=[]
    for row in load_version_tables:
        atable=row['table'].upper(); akey=row.get('key')
        args.log.debug(f'{atable} {akey}')
        if not akey: continue
        akey=akey.upper()
        src_table_cols=set([col.upper() for col in src_table_cols_dict[atable]])
        if key_delim in akey:
            load_version_keys=set(akey.split(key_delim))
            missing_keys=load_version_keys - src_table_cols
            if missing_keys: 
                args.log.info(f" {missing_keys} keys not in {atable}")
                all_missing_keys.append([atable,missing_keys])
        elif akey not in src_table_cols:
            args.log.info(f"{akey} key not in {atable}")
            all_missing_keys.append([atable,akey])
            
    return all_missing_keys
    

def replace_table(args,row):
    args.dbcon.exe(f"CREATE OR REPLACE TABLE {args.tgtdb}.{args.tgtschema}.{row['table']} clone {args.snow_srcdb}.{args.srcschema}.{row['table']}")


def append_table(args,srctable_cols,tgttable_cols,row):
    '''
    1. Column removed -done
    2. Column added -done
    3. Column removed and new column added(could be rename) -done
    4. Primary key removed - done, handled in main
    '''
    new_cols_to_add=set(srctable_cols).difference(tgttable_cols)
    removed_cols=set(tgttable_cols).difference(srctable_cols)
    if new_cols_to_add: args.log.info(f'{new_cols_to_add} were added to source')
    if removed_cols: args.log.info(f'{removed_cols} were removed from source')

    fq_src=f"{args.snow_srcdb}.{args.srcschema}.{row['table']}"
    fq_tgt=f"{args.tgtdb}.{args.tgtschema}.{row['table']}"
    table_cols_str=','.join(srctable_cols)
    cols_type_to_add=[]
    for col in new_cols_to_add:
        col=col+' TEXT'
        cols_type_to_add.append(col)

    list_of_calls_to_add = ','.join(cols_type_to_add)

    if list_of_calls_to_add: 
        alter_sql = f"ALTER TABLE {fq_tgt} ADD {list_of_calls_to_add}"
        args.dbcon.exe(alter_sql)
    insert_sql =f"INSERT INTO {fq_tgt} ({table_cols_str}) SELECT {table_cols_str} from {fq_src}"
    args.dbcon.exe(insert_sql)


#def move_data(args,src_table_cols_dict,tgt_table_cols_dict,row):
def main_parallel(allargs):
    try:
        
        args,dbcon_dict,src_table_cols_dict,tgt_table_cols_dict,row=allargs
        table=row['table']
        args.log=inf.setup_log(args.logdir,app=f'child_{table}',prefix='     _')
        

        #schema compare
        #datatype compare
        #insert strategy
       
        srctable_cols=src_table_cols_dict[table]
        tgttable_cols=tgt_table_cols_dict[table]
        if not srctable_cols:
            return f'{table}: Could not find columns'
        # raise ValueError(f'Could not find columns for {table}')
        args.dbcon=dblib.DB(dbcon_dict,log=args.log,port=dbcon_dict.get('port',''))
        operation=row.get('drop')
        if operation=='y':
            replace_table(args,row)
        elif not tgt_table_cols_dict[row['table']]:
            replace_table(args,row)
        elif operation=='append':
            append_table(args,srctable_cols,tgttable_cols,row)
        else: 
            args.log.info(f'{table}: Unsupported Operation {operation}!')
            return f'{table}: Unsupported Operation {operation}!'
            #raise ValueError('Unsupported Operation!')
        args.log.info(f'Done')
        return table
    except:
        errormsg=inf.geterr()
        args.log.info(f'Error {table}')
        args.log.debug(errormsg)
        return f'{table}: {errormsg}'



#---------- Standard setup

def process_results(args,results):
    errors=[]
    for idx,result in enumerate(results):
        if ':' in result:
            errors.append(result.split(':',1)[0])
    args.log.info(f'Out of {idx} tables found {len(errors)} errors')
    return errors


def process_args():

    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    #datadir = "I:/Data_Lake/CAM"

    parser = argparse.ArgumentParser(
        description='command line args', epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00", add_help=True)
    # required
    parser.add_argument( '--srcdir', required=True, help='src dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt db eg. DEV_SOURCE')
    parser.add_argument( '--load_version', required=True, help='csv file that defines loading for a schema')
    #boolean
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )
    # optional
    parser.add_argument( '--snow_srcdb', required=False,default='', help='src db eg. DEV_STAGE')
    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')
    
    args = parser.parse_args()

    if args.eldir=='dev': 
        args.eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"

    args.root=Path(__file__).resolve().parent.parent
    args.loaddir=args.root/'bwcloads'
    
    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srcdata
    #use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args)
    args.load_file=args.loaddir/args.srcenv/args.srcdb/f'{args.srcschema}_{args.load_version}.csv'

    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

    
def main():
    '''
    '''
    try:
        args=None
        args = process_args()
        #Only needed for Snowflake
        srcdb_dict=dbsetup.Envs[args.srcenv][args.srckey]
        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    
        if args.snow_srcdb:
            srcdb=args.snow_srcdb
        dbcon=dblib.DB(tgtdb_dict,log=args.log,port=tgtdb_dict.get('port',''))
        '''CREATE SCHEMA  IF NOT EXISTS UAT_SOURCE.dbmoit00;'''
        sql=f'CREATE SCHEMA IF NOT EXISTS {args.tgtdb.upper()}.{args.tgtschema.upper()}'
        dbcon.exe(sql)
        #srcdb=dbcon
        tgtdb=args.tgtdb
           
        #src_table_cols_dict,src_view_cols_dict=get_table_cols(args,db=args.srcdb)
        load_version_tables=get_load_version_dict(args)
        load_version_tables_dict={}
        src_table_cols_dict=get_table_cols(args,load_version_tables,dbcon=dbcon,db=srcdb)
        tgt_table_cols_dict=get_table_cols(args,load_version_tables,dbcon=dbcon,db=tgtdb)
        all_missing_pks=check_pks(args,load_version_tables,src_table_cols_dict)

        if all_missing_pks: raise ValueError(f"!!! No tables loaded - following tbl_dicts missing {all_missing_pks} !!!")

        all_args=[]
        for row in load_version_tables:
            table_args=[args,tgtdb_dict,src_table_cols_dict,tgt_table_cols_dict,row]
            all_args.append(table_args)
        
        results=inf.run_parallel(main_parallel,all_args,parallel=15)
        errors=process_results(args,results)
        if errors:
            raise ValueError(f'Bad Tables {errors}')
        #for row in load_version_tables:
        #    load_version_tables_dict[row['table'].upper()]=row
        #    move_data(args,src_table_cols_dict,tgt_table_cols_dict,row)
 
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
Latest Example:
python run_extract.py --srcdir /uat/oracle_etl/pub1/PCMP --load_version dim --parallel 15
python run_load.py --srcdir /uat/oracle_etl/pub1/PCMP --tgtdir /uat/snow_etl/UAT_STAGING/PCMP --load_version dim --parallel 15
python run_create_source_snowflake.py --srcdir /uat/oracle_etl/pub1/PCMP --tgtdir /uat/snow_etl/UAT_SOURCE/PCMP --load_version dim --snow_srcdb UAT_STAGING
python run_createviews_snowflake.py --srcdir /uat/oracle_etl/pub1/PCMP --snow_srcdb UAT_SOURCE --tgtdir /uat/snow_etl_prod/PREPROD_EDW/PCMP --load_version dim 

'''