
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

def get_tables(args):
    '''
    '''
    src_tables=args.con.get_tables(args.tgtschema)
    table_cols_dict={}
    for table in src_tables[:2]:
        src_columns = args.con.get_cols(args.tgtschema,table)
        table_cols_dict[table] =src_columns

    return table_cols_dict


    print(src_columns)

def create_views(args,table_cols_dict):
    for table,cols in table_cols_dict.items():
        sql =create_view_sql(args,table,cols)
        print(sql)
        
    

def create_view_sql(args,table,cols):
    print(table,cols)
    print(args.tgtschema)
    #print(f'create or replace view a8789_old.'+ table + ' as select * from ' + args.tgtschema+'.'+ table)
    print(args.con.dbtype)
    view = f'VIEW_{table}'
    cols_str=',\n\t'.join(cols)
    if args.con.dbtype=='vertica':
        sql=f''' CREATE VIEW {args.tgtschema}.{view}  as \n\tselect \n\t{cols_str} from {args.tgtschema}.{table}'''
    else:
        raise ValueError('database connection not supported')
    
    return 'sql'
    





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
    #parser.add_argument( '--del_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    # optional
    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )

    args = parser.parse_args() 
    #build: args.tgtdata,args.tgtlog,args.srcd
    # ata,args.srcdata, etc.
    #use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args) 
    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

def main():
    args = process_args()
    dbdict = dbsetup.Envs[args.tgtenv][args.tgtdb]
    with  dblib.DB(dbdict,log=args.log,port=dbdict.get('port','')) as args.con:     #args.con.closedb()  NOT NEEDED!
        table_cols_dict=get_tables(args)
        create_views(args,table_cols_dict)
        
    
    args.log.info('done')


if __name__=='__main__':
    main()