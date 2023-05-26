
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
    libpath=str(prog_path.parent.parent.parent)
    sys.path.append(libpath)
    print('using path',libpath)

# #local libs
set_libpath()
from bwcenv.bwclib import dblib,inf,extract_info
from bwcsetup import dbsetup

def make_sql(args,tgt_table):
    #table_list = get_tables(args)
    #print(table_list)    
    if args.con.dbtype=='vertica':
        sql= f''' TRUNCATE TABLE {args.tgtschema}.{tgt_table};'''
    else:
        raise ValueError('database connection not supported')
    #print(sql)
    return sql

#---------- Standard setup

def process_args():

    eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    #datadir = "I:/Data_Lake/CAM"

    parser = argparse.ArgumentParser(
        description='command line args', epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00", add_help=True)
    # required
    #parser.add_argument( '--srcdir', required=True, help='tgt dir for data and logs')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    #boolean
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )
    #parser.add_argument( '--del_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    # optional
    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')
    parser.add_argument('--tablesuffix', required=True, help='default directory for logging, data files, etc')

    args = parser.parse_args() 
    if not args.tablesuffix.strip():
        raise ValueError('Suffix is required')
    #build: args.tgtdata,args.tgtlog,args.srcd
    # ata,args.srcdata, etc.
    #use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args) 
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

def main():
    '''  This Program truncates all tables in a schema based in suffix. 
        Suffix is needed because we do not want to truncate wrong tables  '''
    args = process_args()
    dbdict = dbsetup.Envs[args.tgtenv][args.tgtkey]
    suffix=args.tablesuffix.upper()
    with  dblib.DB(dbdict,log=args.log,port=dbdict.get('port','')) as args.con:     #args.con.closedb()  NOT NEEDED!
        tgt_tables =args.con.get_tables(args.tgtschema)
        print(tgt_tables)
        truncate_sql=''
        for tgt_table in tgt_tables:
            if  tgt_table.endswith(suffix):
                truncate_sql = make_sql(args,tgt_table)
                args.con.exe(truncate_sql)
            
            else:
        
                args.log.debug(f'Skipping {tgt_table}')
                continue
        
        
        
        #create_sql(args,tables)
        #create_truncate_sql(args,table)
        
    
    args.log.info('done')

if __name__=='__main__':
    main()

