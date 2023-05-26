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


def fixcols(args,cols):
    reserve={"SCHEMA","START","STATUS","TABLE"}
    chars=set('!@#$%^&*')

    cleaned_cols=[]
    for col in cols:
        if any((char in chars)for char in col):
            col=f'"{col}"'
        elif col in reserve:
            col=f'"{col}"'
        cleaned_cols.append(col)

    return cleaned_cols

def get_srcviewmap(args,db=''):
    '''
    db is a optional arugument because in snowflake the views can be created in another database.
    
    {'ACCOUNT_TYPE': ['ACCT_TYP_CD', 'ACCT_TYP_NM', 'ACCT_TYP_VOID_IND', 'BWC_DW_EXTRACT_TS', 'BWC_DW_LOAD_KEY', 'BWC_DW_LOAD_START_DTTM'],
     'BWCSTAGEDW_APC_INFO': ['APC_AMT', 'APC_BGN_DATE', 'APC_CODE', 'APC_DESC', 'APC_END_DATE', 'APC_STS_IND_CODE', 'APC_STS_IND_DESC', 'BWC_DW_LOAD_START_DTTM', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE'], 
     'BWCSTAGEDW_API_SUM': ['API_RCPT_DATE', 'BWC_DW_LOAD_START_DTTM', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE', 'MCO_ID_NO', 'MCO_NAME', 'NTWRK_ID_NO', 'TOTAL_FAIL_QTY', 'TOTAL_PASS_QTY', 'TOTAL_SBMTD_QTY'], 
     'BWCSTAGEDW_DIAG': ['BWC_DW_LOAD_START_DTTM', 'CARE_HDR_ID_NO', 'CHANGE_INDICATOR', 'DW_CNTRL_DATE', 'ICD_CODE', 'ICD_SQNC_NO', 'INVC_NO', 'PRNCP_ICD_FLAG', 'PRSNT_ADMSN_TEXT']}
    '''
    
    src_tables=args.srccon.get_objects(args.srcschema,db)


    table_cols_dict={}
    #view_cols_dict={}
    mtable=[]
    tviews=[]
    
    for idx,atable in enumerate(src_tables):
        if args.table_prefix:
            if not atable.startswith(args.table_prefix):
                continue

        vw=(atable[len(args.table_prefix):])
          
        mtable.append(atable)
        tviews.append(vw)
        tcols=args.srccon.get_cols(args.srcschema,atable,db)
        
        table_cols_dict[atable]=fixcols(args,tcols)

        # vcols=args.srccon.get_cols(args.srcschema,vw,db)
        
        # view_cols_dict[vw]=fixcols(args,vcols)
        
          
    #print(mtable,tviews)
        

    #return table_cols_dict,view_cols_dict
    return table_cols_dict


def create_view_sql(args,src_table,src_cols,tgt_view,src_db='',tgt_db=''):
    if args.tgtcon.dbtype=='vertica' or args.tgtcon.dbtype=='snowflake':
        
        sql = f"CREATE VIEW {tgt_db}.{args.tgtschema}.{tgt_view }    AS SELECT\n  "
        if args.force:
            sql = f"CREATE OR REPLACE VIEW {tgt_db}.{args.tgtschema}.{tgt_view }    AS SELECT\n  "

        rows=[]
        for col in sorted(src_cols):
            rows.append(f'{col} ,')
        rows[-1]=rows[-1].strip(',')

        sql += '\n     '.join(rows)
        sql+=f'\n from {src_db}.{args.srcschema}.{src_table};'
    else:
        raise ValueError(f'Database not supported {args.tgtcon.dbtype}')

    
    return sql

def save_sql(args,table,sql):
    fname=Path(args.tgtdata)/(table+'.sql')
    fname.write_text(sql) 
    print(fname)
    

def create_views(args,src_table_cols_dict,tgt_views,src_db='',tgt_db=''):
    '''
    assumes same database and connection is used for src and tgt!
    First check to see if Target view already exists
    If it does, verify nothing has changed then skipped.
    If it does not, create a view.
    '''
    #print(src_table_cols_dict)
    args.log.info(f'in base create views {src_table_cols_dict}')
    for src_table,src_cols in src_table_cols_dict.items():
        tgt_table=src_table
        if args.table_prefix:
            
            if not args.keep_prefix:
                #remove prefix when comparing source and target
                if tgt_table.startswith(args.table_prefix):
                    tgt_table=tgt_table[len(args.table_prefix):]
            #print(f'tgt_table={tgt_table}')[P]
        
        #Verify source and target are in sync.
        if not args.force and tgt_table in tgt_views:
            args.log.info(f'{tgt_table=} present for {src_table=}, skipping') 
            tgt_cols = args.tgtcon.get_cols(args.tgtschema,tgt_table,db=tgt_db)
            if sorted(src_cols) != sorted(tgt_cols):
                args.log.warning(f'col mismatch {src_table}\n\tsrc {src_cols}\n\t{tgt_cols}')
            continue
            # only use for testing!
            # if args.con.dbtype=='vertica':
            #     args.con.exe(f"drop TABLE IF EXISTS {args.tgtschema}.{tgt_table}")
        # Target view is not present then go ahead and create it.
        sql=create_view_sql(args,src_table,src_cols,tgt_table,src_db,tgt_db)
        save_sql(args,tgt_table,sql)

        args.tgtcon.exe(sql)



#---------- Standard setup

def process_args():

    #eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"
    #datadir = "I:/Data_Lake/CAM"

    parser = argparse.ArgumentParser(
        description='command line args', epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00", add_help=True)
    # required
    parser.add_argument( '--srcdir', required=True, help='tgt dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    #boolean
    parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )
    parser.add_argument( '--force', action='store_true', help='Force creation of views again' )
    # optional
    parser.add_argument('--table_prefix', required=False, default='', help='only process tables with this prefix, auto adds _')

    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')
    

    args = parser.parse_args() 
    
    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srcdata
    #use_load_key=False, find_src_load_key=False 
    if args.table_prefix:
        args.table_prefix=args.table_prefix.upper()+'_'
    inf.build_args_paths(args)
    
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

    

def main():
    '''
    
    Namespace(cleanlog=False, eldir=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL'),
    keep_prefix=True, log=<Logger parent (DEBUG)>, 
    logdir=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devv/DEV_VIEWS/PH__PCMP/logs'),
    srcdata=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devt/DEV_SOURCE/PH__PCMP/data'), 
    srcdb='DEV_SOURCE',
    srcdb_dict={'server': 'dza60922.us-east-2.aws', 
      'db': 'DEV_SOURCE', 'login': 'X10140138', 
    , 'type': 'snowflake', 'warehouse': 'WH_BI'}, 
    tgtdir=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devv/DEV_VIEWS/PH__PCMP'),
    tgtenv='dev', 
    tgtkey='snowflake_devv', 
    tgtlog=WindowsPath('I:/Data_Lake/IT/ETL/a83789/EL/dev/snowflake_devv/DEV_VIEWS/PH__PCMP/logs'),
    tgtschema='PH__PCMP')
    
    '''
    try:
        args=None
        args = process_args()
        #Only needed for Snowflake
        srcdb_dict=dbsetup.Envs[args.srcenv][args.srckey]
        tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
    
        args.srccon=dblib.DB(srcdb_dict,log=args.log,port=srcdb_dict.get('port',''))
        args.tgtcon=dblib.DB(tgtdb_dict,log=args.log,port=tgtdb_dict.get('port',''))
            
        #src_table_cols_dict,src_view_cols_dict=get_srcviewmap(args,db=args.srcdb)
        src_table_cols_dict=get_srcviewmap(args,db=args.srcdb)
        tgt_views=args.tgtcon.get_objects(args.tgtschema,db=args.tgtdb)

        create_views(args,src_table_cols_dict,tgt_views,args.srcdb,args.tgtdb)
    except:
        if args:
            args.log.info(inf.geterr())
        sys.exit(1)



if __name__=='__main__':
    main()


'''
python run_createviews.py  --srcdir /dev/vertica_me/nielsenjf --tgtdir /dev/vertica_me/nielsenjf  -table_prefix bwc_stage
'''