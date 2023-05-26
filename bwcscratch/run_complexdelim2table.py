#!/usr/bin/env python

#/usr/bin/scl enable python27 -- /path/to/python/script.py


#std libraries
import time,os,base64,datetime,time,sys,socket,gzip
import shutil

#local libraries
path='/etl/repository/hgmasters/bwclib/lib/'
sys.path.append(path)



#libraries
from bwclib import dbsetup, dbmeta, dblib, inf, process_schema, dwargs, dbmeta_data, inf2


#'%Y-%m-%d %H:%M:%S.%f
def load_files(srcdir,tgtdb,tcon,args,dir_info,delim,term,log_obj):
            success_cnt=0
            error_cnt=0
	    error_flag=False

            for srcfile in os.listdir(srcdir)[:]:
                    try:
                        if not srcfile.endswith('.final.gz'): continue
                        tgt_tbl=srcfile.replace('.txt.final.gz','')
                        tgt_tbl=tgt_tbl.replace('.txt.final','')
                        tgt_tbl=tgt_tbl.replace('dbo.','')
                        src_file= srcdir + srcfile
                        print 'table:',tgt_tbl,'file:',src_file
                        # src_file= '/etl/prod_i_drive/IT/ETL/RISK_CONTROL/Extract/isiContacts.csv'
                        #tgt_tbl= 'isiContacts'            

#                        file2table_vsql(tgtdb,tcon,src_file,args.t_schema,tgt_tbl,dir_info['log_dir'],args.truncate,True, delim=delim,term=term,header='',log_obj=log_obj)
                        file2table_vsql(tgtdb,tcon,src_file,args.t_schema,tgt_tbl,dir_info['log_dir'],args.truncate,False, delim=delim,term=term,header='',log_obj=log_obj)
                        print 'completed-',tgt_tbl, 'file-', src_file
			print '********************************************************************************************'
                        
                        
                        success_cnt+=1
                    except:
                        error = inf.err()
                        print(''); print('EXCEPTION ERROR:' + tgt_tbl+':'+error)
                        log_obj.error(error)
                        error_flag=True
                        print 'failed-',tgt_tbl, 'file-', src_file
                        
                        error_cnt+=1
            if error_flag:
                raise ValueError('table load failed success:%s error:%s'%(success_cnt,error_cnt))
    

def fix_files(srcdir,delim,term,final_delim,final_term):

    for srcfile in os.listdir(srcdir)[:]:
	    if srcfile.endswith('.final.gz'):os.remove(srcdir+srcfile)
	    if srcfile.endswith('.final'):os.remove(srcdir+srcfile)

    files_to_convert=os.listdir(srcdir)[:]

    for srcfile in files_to_convert:
        final_file=srcfile+'.final.gz'
        fr=open(srcdir+srcfile,'r')
        fw=gzip.open(srcdir+final_file,'wb',compresslevel=1)
	data=fr.read()

	#get rid of all bad delim and term characters
	data=data.replace(final_term,' ')   #replace  bad \n with space
	data=data.replace(final_delim,' ')

	#convert to vsql single character delim
	data=data.replace(delim,final_delim)
	data=data.replace(term,final_term)  #replace {-} with \n 

	fw.write(data)
        fw.close()
	fr.close()


def file2table_vsql(tgt,tcon,fname,t_schema,t_table,log_dir,truncate=False,uncompress=False, log_obj='',delim='\\t',term='\\n',header=''):
    '''
    builds vsql copy command
    copy_dict={'db':db,'schema':tgt_schema,'login':login,'passwd':passwd}
    cur.execute("COPY foo FROM STDIN")   
    '''
    log_obj.info('now running vsql')

    base_fname=os.path.basename(fname)
    now=datetime.datetime.now()
    nowstr=now.strftime('%Y-%m-%d_%H-%M-%S')

    login=tgt['login']
    passwd=base64.decodestring(tgt['passwd'])
    db=tgt['db']
    #

    if truncate:
        sql='truncate TABLE %s '%(t_schema+'.'+t_table)
        tcon.exe(sql)

    VSQL='/opt/vertica/bin/vsql -C --echo-all -U '+login+' -w '+passwd+' -d '+db

    SQL=""" -c "copy %s.%s FROM LOCAL '%s' """%(t_schema,t_table,fname)

    direct=''
    fsize=os.path.getsize(fname)
    if fsize>100000:
        direct='DIRECT'

#    OPTIONS=" GZIP NO ESCAPE DELIMITER E'%s' NULL '' RECORD TERMINATOR E'%s' "%(delim,term)
    OPTIONS=" GZIP NO ESCAPE DELIMITER E'\\006' NULL '' RECORD TERMINATOR E'\\007' "
    if uncompress:
        OPTIONS="NO ESCAPE DELIMITER E'\\006' NULL '' RECORD TERMINATOR E'\\007' "

    OPTIONS+="TRAILING NULLCOLS "+direct+" ENFORCELENGTH EXCEPTIONS '%s/%s.exceptions_%s' "%(log_dir,base_fname+'_'+t_table,nowstr)
    OPTIONS +="REJECTMAX 1000000 REJECTED DATA  '%s/%s.rejects_%s' "%(log_dir,base_fname+'_'+t_table,nowstr)
    if header: OPTIONS +='SKIP 1'
    OPTIONS+=';"'
    command=VSQL+' '+SQL+' '+OPTIONS

#    print command


    ret,out,err=inf.subproc(command)

    if log_obj:
        log_obj.info('return:\t'+str(ret))
        log_obj.info('stdout:\t'+str(out))
        log_obj.info('stderr:\t'+str(err))

    if ret:
        raise ValueError('stage to table '+t_table+' failed:'+err)
    if out:
        if 'Rows Loaded' in out:
            rejects='%s/%s.rejects_%s'%(log_dir,base_fname+'_'+t_table,nowstr)
            print 'checking for',rejects
            if os.path.exists(rejects) and os.path.getsize(rejects):
                raise ValueError('Rows rejected:filesize'+str(os.path.getsize(rejects)))
            else:
                return out.split()[-3]
        else:
            raise ValueError('no rows loaded '+table+' failed:'+out)
    else:
        raise ValueError('Unknown error '+table+' failed:'+out+' '+err)


def main():

    user=os.environ['USER']
    host=socket.gethostname().split('.')[0]

#--t_environment prod  --tgtdb vertica --topdir /etl/dwdata/tmp --s_schema DW_REPORT --t_schema DW_REPORT 

    parser=dwargs.make_elargs(
        'extract script for EL process',
        src=False,
        tgt=True,
        ops=['vsql','custom'],
        srcdir='required',
        delim='optional',
        term='optional',
        header='optional',
        truncate='optional',
        uncompressed='optional',
        load_type='required',
        topdir='optional',
    )

    ##########setup based off of ARGUMENTS
    args = parser.parse_args()
    print args
    tgtdb = dbsetup.Envs[args.t_environment][args.tgtdb]
    t_schema = args.t_schema
    ##########                                                                                                                                                                                                                     
    #make databases connection                                                                                                                   
    tcon=dblib.DB(tgtdb,log=True,port=tgtdb.get('port',''))

    load_key=dbmeta.get_load_key(tgtdb,args.t_schema,load_type=args.load_type)

    dir_info=inf2.get_dirs('dw_el',load_key,args.load_type,topdir=args.topdir)
    root_dir=dir_info['root_dir']
    log_obj=dir_info['log_obj']

    try:
        
        print 'logging',dir_info['log_dir']
        log_obj.info('logging to:'+dir_info['log_dir'])
        log_obj.info('reading from:'+ args.srcdir)

        delim=args.delim
        if not delim: 
		delim='\t'

	term=args.term
	if not term:
		term='\r\n'

	if term=='\\n':
		term='\n'

	final_delim='\06' 
	final_term='\07'

        srcdir=args.srcdir

        if args.operation=='vsql':
            fix_files(srcdir,delim,term,final_delim,final_term)
            load_files(srcdir,tgtdb,tcon,args,dir_info,'','',log_obj)
                
        elif args.operation=='custom':
            raise ValueError('not supported')

        log_obj.info('done')
    except:
        error = inf.err()
        print(''); print('EXCEPTION ERROR:' + error)
        log_obj.error(error)
        sys.exit(1)

        
if __name__=='__main__':
    main()


'''

python  ~/bwclib/run/run_complexdelim2table.py --delim "|"  --operation vsql --t_environment dev_cognos --tgtdb vertica --t_schema DW_ACTUARIAL  --load_type PCMP  --srcdir  /etl/i_drive/IT/ETL/TRANSFER/DB/PAYMENT_CODING/ 
'''

