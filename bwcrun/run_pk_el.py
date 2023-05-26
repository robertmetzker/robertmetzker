# Standard Libraries shared/used by BWC
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime,collections
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

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

import threading, queue

def process_args():
    '''
    python e:/py/extract.py --env dev --db cam --schema BASE --table ADMIN_PMT_PCT 

    '''
    #eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    eldir=f"E:/EXTRACTS/{os.environ['USERNAME'].replace('_x','')}/EL"

    parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --srcdir /dev/cam/base",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='/env/dbtype/dbname/schema')
    parser.add_argument( '--tgtdir', required=True,help='/env/dbtype/dbname/schema')
    #optional
    parser.add_argument( '--tables', required=False,help='limited list of tables to process')
    parser.add_argument( '--eldir', default=eldir,help='default directory to dump the files')
    parser.add_argument( '--level', default=1,help='where in call chain this is for logging')
    #boolean
    parser.add_argument( '--cleanlog',action='store_true', help='changes the logger to overwrite mode')
    parser.add_argument( '--rerun',action='store_true', help='re-extracts source primary keys')
    args = parser.parse_args()

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srclog,args,srcenv
    inf.build_args_paths(args)
    args.prefix=''

 # Directory setup based on the provided /env/conn/SCHEMA provided at run-time
 # uses the YYYY_MM_DD format at time of execution to generate the LOAD_KEY under which all other folders will be created
    args.logdir=args.srclog
    args.csvdir=args.srcdata/'extracts'

 # Directories will be created if they do not already exist.
    args.csvdir.mkdir(parents=True, exist_ok=True)
    args.log=inf.setup_log(args.logdir,app='parent',prefix=args.level*'\t',cleanlog=args.cleanlog)
    args.log.info(f'{sys.argv[0]} launched with arguments: {sys.argv[1:]}')
    args.log.debug(f'Saving files to: {args.csvdir} {args.logdir}')
    args.log.debug(f'args global settings:{args}')
   
    return args

def get_src_pks(args,fname,key_order,srccon):
    
    if fname.exists() and not args.rerun:
        args.log.info(f'Primary Keys ALREADY extracted from {args.srcdb}.{args.srcschema}')
    else:
        args.log.info(f'Extracting Primary Keys from {args.srcdb}.{args.srcschema}')
        src_primary_keys=srccon.extract_keys(args.srcschema,args.srcdb)

        ordered_src_pks=[]
        for adict in src_primary_keys:
            d = collections.OrderedDict(adict)
            for k in key_order:
                d.move_to_end(k)
            ordered_src_pks.append(adict)

        args.log.info(f'Writing Primary Keys to file: {fname}')
        inf.write_csv(fname,ordered_src_pks,sortit=False,log=args.log)

def format_src_pks(fname,srcdbdict):
    primary_keys=list(inf.read_csv(fname,'\t'))

    if srcdbdict['type']=='vertica':
        pk_dict_list=[]
        for pk in primary_keys:
            key_dict={}
            for k,v in pk.items():
                key_dict[k]=v
            pk_dict_list.append(key_dict)
    else:
        pk_dict_list=primary_keys
    return pk_dict_list

def get_tgt_pks(args,key_order,tgtcon):
    tgt_primary_keys=tgtcon.extract_keys(args.tgtschema,args.tgtdb)
    ordered_tgt_pks=[]
    for adict in tgt_primary_keys:
        d = collections.OrderedDict(adict)
        for k in key_order:
            d.move_to_end(k)
        ordered_tgt_pks.append(adict)
    return ordered_tgt_pks

def build_output_list(ordered_tgt_pks,pk_dict_list,args):
    tgt_pk_dict_list=[]
    for row in ordered_tgt_pks:
        new_dict={}
        for k in row.keys():
            if not 'SCHEMA' in k:
                new_dict[k]=row[k]
        tgt_pk_dict_list.append(new_dict)

    src_pk_dict_list=[]
    for row in pk_dict_list:
        new_dict={}
        for k in row.keys():
            if not 'SCHEMA' in k:
                new_dict[k]=row[k]
        src_pk_dict_list.append(new_dict)

    output_pk_dict_list=[]
    for spk in src_pk_dict_list:
        if not spk in tgt_pk_dict_list:
            output_pk_dict_list.append(spk)
            spk['SCHEMA_NAME'] = args.tgtschema        
    for tpk in tgt_pk_dict_list:
        if not tpk in src_pk_dict_list:
            output_pk_dict_list.append(tpk)
            tpk['SCHEMA_NAME'] = args.tgtschema
 
    distinct_tblschema=[]
    for row in output_pk_dict_list:
        distinct_combo = row['SCHEMA_NAME'] + '.' + row['TABLE_NAME']
        if not distinct_combo in distinct_tblschema:
            distinct_tblschema.append(distinct_combo)

    pks=[]
    table_list = dict.fromkeys(distinct_tblschema,pks.copy())
    for table in distinct_tblschema:
        for row in src_pk_dict_list:
            row['SCHEMA_NAME'] = args.tgtschema
            if table == row['SCHEMA_NAME'] + '.' + row['TABLE_NAME']:
                pks.append(row['PK_NAME'])
        table_list[table] = list(pks)
        pks=[]

    if not table_list:
        args.log.info('Primary Keys are in sync!')

    distinct_tgt_tables=[]
    for adict in tgt_pk_dict_list:
        table = adict['TABLE_NAME']
        if not table in distinct_tgt_tables:
            distinct_tgt_tables.append(table)
        
    return table_list,distinct_tgt_tables

def run_sql(args,table_list,distinct_tgt_tables,tgtcon):
    missing_tgt=[]
    for key,value in table_list.items():
        try:
            if key.split('.')[1] in distinct_tgt_tables:
                args.log.info(f'Source/Target Primary Key mismatch! Dropping Primary Keys in {args.tgtdb}.{args.tgtschema}')
                remove_sql = f"ALTER TABLE {args.tgtdb}.{args.tgtschema}.{key.split('.')[1]} DROP PRIMARY KEY;"
                tgtcon.exe(remove_sql)
                if value:
                    args.log.info(f'Creating Primary Keys in {args.tgtdb}.{args.tgtschema}')
                    add_sql = f"ALTER TABLE {args.tgtdb}.{args.tgtschema}.{key.split('.')[1]} ADD PRIMARY KEY ({', '.join(value)} );"
                    tgtcon.exe(add_sql)
            else: 
                args.log.info(f'Creating Primary Keys in {args.tgtdb}.{args.tgtschema}')
                add_sql = f"ALTER TABLE {args.tgtdb}.{args.tgtschema}.{key.split('.')[1]} ADD PRIMARY KEY ({', '.join(value)} );"
                tgtcon.exe(add_sql)
        except:
            errmsg=inf.geterr()
            args.log.error(errmsg)
            missing_tgt.append(key.split('.')[1])

    if missing_tgt:
        args.log.info(f'''Tables: {missing_tgt} do not exist in {args.tgtdb}.{args.tgtschema}''')

def main():

    args=process_args()

    srcdbdict = dbsetup.Envs[args.srcenv][args.srckey]
    srccon=dblib.DB(srcdbdict,log=args.log,port=srcdbdict.get('port',''))
    tgtdbdict = dbsetup.Envs[args.tgtenv][args.tgtkey]
    tgtcon=dblib.DB(tgtdbdict,log=args.log,port=tgtdbdict.get('port',''))

    fname = args.csvdir/f'{args.srcschema}.csv'
    key_order = ['SCHEMA_NAME','TABLE_NAME','PK_NAME']

    get_src_pks(args,fname,key_order,srccon)
    
    args.log.info(f'Reading Primary keys from file: {fname}')
    pk_dict_list = format_src_pks(fname,srcdbdict)

    ordered_tgt_pks=get_tgt_pks(args,key_order,tgtcon)

    table_list,distinct_tgt_tables = build_output_list(ordered_tgt_pks,pk_dict_list,args)

    if args.tables:
        table_args = args.tables.split(',')
        limited_list=[]
        for table in table_list.keys():
            if table.split('.')[1] in table_args:
                limited_list.append(table)
        limited_output= {}
        for key,value in table_list.items():
            if key in limited_list:
                limited_output[key]=value
        args.log.info(f'Only running for specified tables: {args.tables}')
        run_sql(args,limited_output,distinct_tgt_tables,tgtcon)
    
    else:
        args.log.info('Running for all tables in schema')
        run_sql(args,table_list,distinct_tgt_tables,tgtcon)
    
    args.log.info('done')

if __name__=='__main__':
    main()

'''
python run_pk_el.py --srcdir /dev/vertica_testprod_etl/RPD1/BWCMISC --tgtdir /dev/snow_etl/DBTEST/DBT_PBALZER
python run_pk_el.py --srcdir /uat/oracle_etl/pub1/BWC_ETL --tgtdir /dev/snow_etl/DBTEST/DBT_PBALZER
python run_pk_el.py --srcdir /dev/cam_etl/CPD1/BASE --tgtdir dev/snow_etl/DBTEST/DBT_PBALZER
python run_pk_el.py --srcdir /dev/db2nt/DBNT/DBMONT00 --tgtdir dev/snow_etl/DBTEST/DBT_PBALZER

'''