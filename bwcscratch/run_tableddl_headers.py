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
    root=prog_path.parent.parent.parent
    pyversion=f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    print('using path',root,pylibpath)

set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup


def convert_dtype(args,dtype):
    if 'NUMBER' in dtype or 'NUMERIC' in dtype:
        if '(' not in dtype:
            val='NUMBER'
        else: 
            prec_scale=dtype.split('(')[1].replace(')','')
            if ',' in prec_scale:
                str_prec,str_scale=prec_scale.split(',')
                prec=int(str_prec);scale=int(str_scale)
            else:
                prec=int(prec_scale);scale=0

            prec=int(prec);absprec=abs(prec);scale=int(scale);absscale=abs(scale)

            if absscale>15: val='float' 
            #elif prec==0 and prec<scale and scale!=0:  val='NUMERIC(36,6)' #increasingly expensive for p=37, 58, 67, etc., where p <= 1024
            elif prec==0 and scale<=0: val='INTEGER'
            
            elif absprec>38: val=f"NUMERIC(38,{scale})"
            
            else:
                val=f"NUMERIC({prec},{scale})"
    elif 'INT8' in dtype or 'FLOAT8' in dtype:
        val='NUMBER'
    elif 'DATE' in dtype:
        val='DATE'
    elif 'TIMESTAMP' in dtype:
        val='TIMESTAMP'
    elif 'TIME' in dtype:
        val='TIME'
    else: val='TEXT'

    args.log.info(f'Setting {dtype} to {val}')

    return val

def ddl2rows(args,ddl):
    rows=ddl.split('\n')
    new_rows=[]
    for row in rows:
        row=row.strip()
        if not row: continue
        if row.startswith('CREATE '): continue
        if row=='(': continue
        if row ==');' : break
        row=row.strip(',')
        cols=row.split()
        name=cols[0];dtype=cols[1]
        new_dtype=convert_dtype(args,dtype)
        new_rows.append((name,new_dtype))
    return new_rows

def make_header(args,rows,table):
    header_row={}
    cols=[]
    for row in rows:
        cols.append(row[0])
    header_row={'table': table,'header':','.join(cols)}
    return header_row

def make_dtypes(args,rows,table):
    table_cols=[]
    for row in rows:
        table_cols.append({'table':table,'column':row[0],'dtype':row[1]})
    return table_cols

def process_args():
    '''
    '''

    #etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"


    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument( '--srcdir', required=True, help='tgt dir for data and logs')
    #parser.add_argument( '--presentdb', required=True, help='tgt dir for data and logs') 
    #parser.add_argument( '--dtypedb', required=True, help='tgt dir for data and logs') 
    #parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument('--sql',required=False, help='default directory for all logs, data files')
    #boolean
    #parser.add_argument( '--keep_prefix', default=False,action='store_true',help='if there is a table prefix, remove it viewname')
    #optional
    parser.add_argument('--eldir', required=False, default=eldir,help='default directory for all logs, data files')
    parser.add_argument('--parallel', required=False, default=4,help='number of parallel processes')
    #
    args= parser.parse_args()
    args.parallel=int(args.parallel)

    args.root=Path(__file__).resolve().parent.parent
    #args.loaddir=args.root/'bwcpresent'

    inf.build_args_paths(args)
    
    args.logdir=args.srclog
    args.log=inf.setup_log(args.logdir,app='parent')

    return args

def main():
    try:
        args=None
        args=process_args()

        srcdb_dict=dbsetup.Envs[args.srcenv][args.srckey]
        srccon = dblib.DB(srcdb_dict, log=args.log, port = srcdb_dict.get('port',''))

        #tgtdb_dict=dbsetup.Envs[args.tgtenv][args.tgtkey]
        #tgtcon = dblib.DB(tgtdb_dict, log=args.log, port = srcdb_dict.get('port',''))

        header_fname=args.logdir/f'{args.srcschema}_header.txt'
        dtype_fname=args.logdir/f'{args.srcschema}_dtype.txt'

        table_headers=[]
        table_dtypes=[]
        for table in srccon.get_tables(args.srcschema,db=args.srcdb):
            #input(f'\n\nfix {args.tgtdb.upper()}.{args.tgtschema.upper()}.{table}')

            #table='SUMMARY_MONTHLY_AUTHORIZATION'
            try:
                print('at',table)
                ddl=srccon.get_ddl(args.srcschema,table)
                tbl_rows=ddl2rows(args,ddl)

                header=make_header(args,tbl_rows,table)
                table_headers.append(header)

                dtypes=make_dtypes(args,tbl_rows,table)
                table_dtypes.extend(dtypes)
            except:
                print(f' {table} info not avail, skipping {inf.geterr()}')

        print(f'======================={table_headers}')
        inf.write_csv(header_fname,table_headers)
        inf.write_csv(dtype_fname,table_dtypes)

        #select get_ddl('TABLE','DBTEST.X10057301.ACCOUNT_TYPE');

        #result=srccon.get_cols_dtype(args.srcschema,db=args.srcdb)

                   
        # all_args=[(args,sql) for sql in sqls]
        # if args.parallel==1: 
        #     args.log.debug('Running in single threaded mode')
        #     results= [ main_parallel(allarg) for allarg in all_args ]
        # else:
        #     results=inf.run_parallel(main_parallel,args=all_args,parallel=args.parallel,log=args.log)

        # process_results(args,results)

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
python run_copy_tblddl2views.py  --srcdir /dev/vertica_etl/rpd1/dw_actuarial  --tgtdir /uat/snow_etl/uat_views/dw_actuarial --presentdb uat_present --dtypedb rda1_source --eldir c:/temp
python run_copy_tblddl2views.py  --srcdir /dev/vertica_etl/rpd1/DW_EXTERNAL_RISK_CONTROL --tgtdir /uat/snow_etl/uat_views/DW_EXTERNAL_RISK_CONTROL --presentdb uat_present --dtypedb rda1_source --eldir c:/temp

copy /y i:\xfer\nielsenjf\run_tableddl_headers.py .
python run_tableddl_headers.py  --srcdir /prd/vertica_me/rpd1/dw_safety_council --eldir c:\temp


'''
