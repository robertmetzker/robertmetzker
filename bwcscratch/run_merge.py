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


def run_sql(all_args):
    args,sql=all_args

    logname=sql.split('.')[-1].split()[0]
    args.log=inf.setup_log(args.logdir,app=f'child_{logname}')
    args.log.info(f'processing in {args.etldir}')

    srcdb = dbsetup.Envs[args.srcenv][args.srcdb]
    con=dblib.DB(srcdb,log=args.log,port=srcdb.get('port',''))
    tgtdb = dbsetup.Envs[args.tgtenv][args.tgtdb]
    con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    args.log.info('in run_sql')
    row_gen = con.fetchdict(sql)
    
    for idx,row in enumerate(row_gen):
        if idx>2: break
        print(row)
    return logname

### Existing merge code for reference
'''
def build_delete(tgt,tmp,the_keys):

delete from tgt where (a,b,c,d) in (select a,b,c,d from tgt inner join tmp on)
test if inner join can be removed

where ='delete /*+DIRECT*/ from '+tgt+' where ('
for akey in the_keys:
if akey: where+=akey+','
where=where.strip(',')+') in ('

 select=' select '
for akey in the_keys:
if akey: select+=tgt+'.'+akey+','
select=select.strip(',')+' '

join=select+' from '+tgt+' inner join '+tmp+' on '
ands=''
for akey in the_keys:
if akey: ands+=tgt+'.'+akey+'='+tmp+'.'+akey+' and '
ands=ands[:-4]+')'

 sql= where+' '+join+' '+ands
return sql

'''
'''
delete from a82581.test1 where id in (select id from a82581.test3);
insert into a82581.test1 select * from a82581.test3;
'''
def merge_table(args,con,table,src,tgt,pk,cols):
    if con.dbtype=='vertica':
        if not '_TMP' in table:
            print('merging table',table)
            delete_sql=f'delete /*+DIRECT*/ from {tgt}.{table} where ({pk}) in (select {pk} from {src}.{table}_TMP);COMMIT;'
            print(delete_sql)
            con.exe(delete_sql)
            insert_sql=f'insert /*+DIRECT*/ into {tgt}.{table} ({cols}) select {cols} from {src}.{table}_TMP;COMMIT;'
            print(insert_sql)
            con.exe(insert_sql)
            return
        else: 
            pass
    else:
        raise ValueError('database connection not supported')

def process_args():
    '''
    '''

    eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    #required
    parser.add_argument('--srcdir',required=True,help='directory for source data to be compared')
    parser.add_argument('--sql',required=False, help='sql to be executed')
    parser.add_argument('--tgtdir',required=True,help='directory for target data to be merged ')
    #boolean

    #optional
    parser.add_argument('--eldir', default=eldir,help='default directory for all logs, data files')
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )
    #
    args= parser.parse_args()

    #build: args.tgtdata,args.tgtlog,args.srcdata,args.srcdata
    #use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args,use_load_key=True, find_src_load_key=True)

    args.logdir=args.tgtlog
    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'processing in {args.etldir}')

    return args


def main():

    args=process_args()
    if args==None: return 1

    #print(dbsetup.Envs[args.srcenv])

    srcdb = dbsetup.Envs[args.srcenv][args.srcdb]
    ####is a target db needed????
    # tgtdb = dbsetup.Envs[args.tgtenv][args.tgtdb]
    srccon=dblib.DB(srcdb,log=args.log,port=srcdb.get('port',''))
    ####is a target connection needed???? 
    #tgtcon=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    #print(con.dbtype)
###database specific sql to obtain all tables in specified schema###

#    for sql in sqls:
#        src_sqls=[]
    src_table_name = srccon.get_tables(args.srcschema)
    srcschema=args.srcschema
    tgtschema=args.tgtschema
    print('tables to be merged:',src_table_name)
    for table in src_table_name:
        print(table)
        
        src_primary_keys=srccon.get_keys(args.srcschema,table)
        src_col_names=srccon.get_cols(args.srcschema,table)

        pk=(','.join(src_primary_keys))
        cols=(','.join(src_col_names))
        if not src_primary_keys: continue
        
        merge_table(args,srccon,table,srcschema,tgtschema,pk,cols)
        
        

    


if __name__=='__main__':
    main()
