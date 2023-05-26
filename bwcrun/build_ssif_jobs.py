#std libs
import os, sys, types, time, argparse, datetime
from pathlib import Path

#other lib
#
import streamsets.sdk
#pip3 install streamsets~=4.2

# local libs
sys.path.append("../bwclib")
sys.path.append("../../bwcsetup")
import inf
import streamsets_config,streamsets_dbsetup


#-------- infrastructure jobs
def get_pipes(args,pipe_name='ssif'):
    #get relevant pipes
    pipes = {}
    for p in args.control_hub.pipelines.get_all():
        if not p.name.startswith(pipe_name): continue
        pipes[p.name]=p
    return pipes

def list_jobs(args,job_name='ssif',status=''):
    ''' list all jobs'''
    jobs=list(args.control_hub.jobs)
    final_jobs=[]
    for ajob in jobs: 
        if ajob.job_name.startswith(job_name):
            if status:  
                if status!=ajob.status.status: 
                    continue
            #ajob.refresh()
            final_jobs.append(ajob)
    return final_jobs

def del_job(args,job_name, jobs=[],force=False):
    ''' do not delete jobs w\offset'''
    args.log.info(f' try to del jobs with {job_name}')
    inc=0
    if not jobs:
        jobs=list_jobs(args,job_name=job_name)

    del_count=0
    for inc,ajob in enumerate(jobs): 
        if ajob.job_name==job_name:
            args.log.debug(f'{ajob.status},{ajob.job_name}')
            if ajob.status!='INACTIVE': 
                try:
                        args.control_hub.stop_job(ajob)
                        args.log.debug(f'stopped job {job_name}')
                        time.sleep(1)
                except:
                    print(inf.geterr())

            args.control_hub.delete_job(ajob)
            del_count+=1

    args.log.info(f'deleted {del_count} jobs with {job_name}')

def set_job_properties(args,job,jn,sdc_labels):
    '''
    set retry to forever for  self healing
    '''
    #job.maxRetriesForFailedJob=5
    #job.enable_failover = False
    job.enable_failover = True
    job.enable_time_series_analysis = True
    job.description = jn
    job.pipeline_force_stop_timeout = 30000
    job.statistics_refresh_interval_in_millisecs = 60000
    job.data_collector_labels = sdc_labels

def set_permission(args,acl,group_name,actions,subject_type='GROUP'):
    '''
    TODO create useful group id names to use to set
    TODO raise error if cannot find group
    '''

    for row in args.control_hub.groups:
        if row.display_name==group_name:
            permission = acl.permission_builder.build(subject_id=row.group_id, subject_type=subject_type, actions=actions)
            acl.add_permission(permission)
            break
    else:
        args.log.warning('no group found?',group_name)


def start_job(args,p,jn,sdc_labels,prms,tag):
    job_builder = args.control_hub.get_job_builder()
    job = job_builder.build(
        jn, pipeline=p, runtime_parameters=prms, tags=[tag]
    )
    set_job_properties(args,job,jn,sdc_labels)
    args.control_hub.add_job(job)
    set_permission(args,job.acl,args.team_to_run,['READ', 'WRITE', 'EXECUTE'])
    args.log.debug(f'set perms: {args.team_to_run}')
    args.log.info(f"\t created {jn}")
    if not args.nostart:
        result=args.control_hub.start_job(job,wait=False)
        args.log.info(f"\tstarted {jn} ")
    args.log.info(f"done created {jn} ")

#-------- pipeline creator

def build_reduce_jobs(args,pipes,jobs):
    args.log.info(f"#---- START ")
    prms=  {
                "partition": str(0),
                "file_close_interval": str(30),
                "path_stage_2": f"/{args.dcpath}/base/level/t/stage_01",
                "path_stage_3": f"/{args.dcpath}/base/level/t/stage_02"  
    }

    created=0
    for i,p in enumerate(pipes.values()):
        if not p.name.startswith("ssif_reducer"): continue

        sdc_labels = args.sdc
        jn = f'{args.job_prefix}_red_{args.project}_{args.region}_{p.name.replace("ssif_","")}'
        tag=f"{args.project}/{args.region}/ssif_1"

        job_builder = args.control_hub.get_job_builder()
        del_job(args,jn,jobs)
        start_job(args,p,jn,sdc_labels,prms,tag)
 
def build_pub_jobs(args,pipes,jobs):
    args.log.info(f"#---- START ")
    prms=  {
            "path_stage_3": f"/{args.dcpath}/base/level/t/stage_02/0",
            "recovery_path": f"/{args.dcpath}/base/level/t/recovery",
            # "meta_target_database_name_01": args.meta_target_database_name,
            # "meta_target_schema_name_01": args.srcschema,
            "meta_target_database_name_01": args.tgtdb,
            "meta_target_schema_name_01": args.tgtschema,
            "meta_target_warehouse":args.warehouse,
            "meta_target_stage":args.stage,  #${meta_target_stage} ssif_publisher_snowflake
    }

    created=0
    for i,p in enumerate(pipes.values()):
        if not p.name.startswith("ssif_publisher"): continue

        sdc_labels = args.sdc 
        jn = f'{args.job_prefix}_pub_{args.project}_{args.region}_{p.name.replace("ssif_","")}'
        del_job(args,jn,jobs)
        tag=f"{args.project}/{args.region}/ssif_1"
        start_job(args,p,jn,sdc_labels,prms,tag)

        src=f'{prms["path_stage_3"]}'
        tgt=f'{args.tgtdb}.{args.tgtschema}'
        tgtdb=f'tgtdb info:{prms["meta_target_warehouse"]} {prms["meta_target_stage"]}'
        args.log.info(f'\tPATH: src: {src} tgt: {tgt}')
        args.log.info(f'\tOTHER: {tgtdb} recovery: {prms["recovery_path"]}')
        created+=1
    args.log.info(f"#---- DONE created {created} jobs(s)")

def build_mapper_jobs(args,pipes,jobs):
    '''
        add retry logic to control hub 

    '''
    args.log.info(f"#---- START ")
    created=0
    for p in pipes.values():
        if not p.name.startswith("ssif_mapper"): continue

        for i in range(0, args.partnum+1):

            prms = None
            sdc_labels = []

            prms = {
                "partition": str(i),
                "file_close_interval": str(40),
                "path_stage_1": f"/{args.dcpath}/base/level/t/stage_00",
                "path_stage_2": f"/{args.dcpath}/base/level/t/stage_01",
                "parser_route_01_database_name": args.meta_target_database_name,
                "parser_route_01_database_connection": args.jdbc,
                "parser_route_02_database_name": args.meta_target_database_name,
                "parser_route_02_database_connection": args.jdbc,
                "parser_route_03_database_name": args.meta_target_database_name,
                "parser_route_03_database_connection": args.jdbc,
            }
            
            sdc_labels = [args.sdc[i % len(args.sdc)]] 

            jn = f'{args.job_prefix}_pub_{args.project}_{args.region}_{p.name.replace("ssif_","")}_{i:03}'
            del_job(args,jn,jobs)

            tag=f"{args.project}/{args.region}/ssif_1"
            start_job(args,p,jn,sdc_labels,prms,tag)
            created+=1
        args.log.info(f"#---- DONE created {created} pipes")

def build_extract_jobs(args,pipes,jobs,source_dict,i):
    '''

    '''
    args.log.info(f"#---- START ")

    db=source_dict['db'];sch=source_dict['schema']
    table=source_dict['table'];sql=source_dict['sql']
    ssif_load=source_dict['ssif_load']


    p = pipes[ssif_load]
    #input(table)
    if not sql:
        sql = f'select * from {sch}.{table}'

    if p.name == 'ssif_jdbc_simple_bulk':
        prms = {"meta_stream_type": "jdbc-simple",
                "meta_target_database_name": db,
                "meta_target_schema_name": sch,
                "meta_target_table_name": table,
                "meta_source_database_connection_string": args.jdbc, 
                "meta_sql": sql,
                "fs_ba_01_file_close_interval": str(30),            
                "fs_ba_01_meta_partitions": str(args.partnum),
                "fs_ba_01_meta_mount_volume": f"{args.dcpath}/base",           
                "fs_ba_01_meta_mount_bsa": "level",
                "fs_ba_01_meta_env_type": "t"        
            }
    else:
        raise ValueError(f'unsupported load type {p.name}')

    #/${meta_mount_volume}/${meta_mount_bsa}/${meta_env_type}/stage_00/${record:attribute("meta_partitions")
    src_extraction=f'{prms["meta_target_database_name"]}.{prms["meta_target_schema_name"]}.{prms["meta_target_table_name"]}'
    tgt_extraction=f"{prms['fs_ba_01_meta_mount_volume']}/{prms['fs_ba_01_meta_mount_bsa']}/{prms['fs_ba_01_meta_env_type']}/stage_00/{prms['fs_ba_01_meta_partitions']}"
    args.log.info(f'\tPATH: src: {src_extraction} tgt: {tgt_extraction}')

    sdc_labels = [args.sdc[i % len(args.sdc)]]

    jn = f'{args.job_prefix}_ext_{args.project}_{args.region}_{table}_{p.name.replace("ssif_","")}'.lower()
    del_job(args,jn,jobs)
    tag=f"{args.project}/{args.region}/ssif_2"
    start_job(args,p,jn,sdc_labels,prms,tag)

def build_extract_jobs_in_batches(args,ssif_pipes,ssif_jobs,batch_size=10,max_wait=100):
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['db'].startswith('#'): continue

        waits=0
        while True:
            extract_jobs=ssif_jobs=list(list_jobs(args,job_name='ssif_ext'))
            if len(extract_jobs)<batch_size: 
                args.log.info(f'now executing job count {len(extract_jobs)}  < {batch_size}')
                break
            if waits>max_wait:
                args.log.info(f'now executing waited {max_wait}')
                break
            args.log.info(f'sleeping {len(extract_jobs)} >= {batch_size}');time.sleep(1)
            extract_jobs=ssif_jobs=list(list_jobs(args,job_name='ssif_ext',status='ACTIVE'))
            waits+=1

        
        build_extract_jobs(args,ssif_pipes,ssif_jobs,source_dict,i)


def build_all_extract_jobs(args,ssif_pipes,ssif_jobs):
    for i, source_dict in enumerate(inf.read_csv(args.load_file,delim='~')):
        if source_dict['db'].startswith('#'): continue

        extract_jobs=ssif_jobs=list_jobs(args,job_name='ssif_ext')
        args.log.info(f'found {len(extract_jobs)}')
        build_extract_jobs(args,ssif_pipes,ssif_jobs,source_dict,i)


def parse_args():


    eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"

    #proxy = 'https://login:pass@europa:84'
    proxy = 'https://europa:84'
    os.environ['HTTP_PROXY'] = proxy
    os.environ['HTTPS_PROXY'] = proxy

    #args = types.SimpleNamespace()
    parser = argparse.ArgumentParser(description='command line args',epilog="Example:",add_help=True)
    #required
    parser.add_argument( '--srcdir', required=True,help='data source /env/db/schema')
    #boolean
    parser.add_argument( '--nostart', default=False,action='store_true',help='as soon as jobs are created, start them')
    parser.add_argument( '--addinf', default=False,action='store_true',help='(re)create infrastructure jobs')
    #optional
    parser.add_argument( '--eldir', default=eldir,help='default directory for runtime data and logs')
    parser.add_argument( '--load_file', default='',help='data source /env/db/schema')
    parser.add_argument( '--tgt', default='/test/pda1/pcmp',help='data source /env/db/schema')
    parser.add_argument( '--partnum', default=5,help='num partitions')
    parser.add_argument( '--team_to_run', default='ETLTeam' ,help='group to run the jobs')
    parser.add_argument( '--delim', default='~' ,help='group to run the jobs')
    parser.add_argument( '--load_version',default='',help='custom load file to use')
    parser.add_argument( '--load_key', default='',help='load_key to use (defaults to current date as YYYY_MM_DD')
    args = parser.parse_args()

    # ------BWC ------------
    args.bwcroot=Path(__file__).resolve().parent.parent.parent
    args.bwcenv=args.bwcroot/'bwcenv'
    args.configdir=args.bwcroot/'bwcsetup'
    args.loaddir=args.bwcenv/'bwcloads'
    args.cert=args.configdir/'cert/cacert.pem'


    #-- set the load key if not specified
    now=datetime.datetime.now()
    args.now=now
    ymd=now.strftime('%Y_%m_%d') #2021_05_14
    ymd_hms=now.strftime('%Y-%m-%dT%H:%M:%S') #2021-06-28T13:33:08   select to_timestamp('2021-06-28T13:33:08', 'YYYY-MM-DD"T"HH24:MI:SS');
    if not args.load_key: args.load_key=ymd
    args.load_ts=ymd_hms


    #point to cert to allow ssl firewall validation
    os.environ["REQUESTS_CA_BUNDLE"] =  str(args.cert)
    os.environ["SSL_CERT_FILE"] =str(args.cert)

    #job environment
    inf.build_args_paths(args,use_load_key=True, find_src_load_key=True)
    #junk,args.env,args.srcdb,args.srcschema=args.src.split('/') #args.src='/test/pda1/pcmp'
    #junk,junk,args.tgtdb,args.tgtschema=args.src.split('/')

    #setup logging
    args.logdir=args.srclog
    print('logging to ',args.logdir)
    args.log=inf.setup_log(args.logdir)

    #setup source database extraction info
    if args.load_version:
        args.load_file=args.loaddir/f'{args.srcenv}_{args.srcdb}_{args.srcschema}_{args.load_version}.csv'

    args.jdbc=streamsets_dbsetup.databases[args.srcenv]['pda1']

    # ------ Streamsets------------
    args.cred_id=streamsets_config.cred_id 
    args.cred_token=streamsets_config.cred_token
    args.pipeline_name='pipetest'

    args.region = args.load_file
    args.project = args.srcenv
    args.meta_target_database_name = args.srcdb
    args.topology_name = f"{args.project}_{args.region}_ssif_1"
    #args.sdc=['all'] 
    args.sdc=['SDC1']
    args.dcpath='testreplication'
    #updste ssif_publisher parameters
    args.warehouse='WH_BI'
    args.stage='streamstage'  #needs a named internal stage?
    args.job_prefix='ssif'
    return args


def main():

    args=parse_args()
    args.log.info('starting')
    args.control_hub = streamsets.sdk.ControlHub(credential_id=args.cred_id, token=args.cred_token)

    ssif_pipes=get_pipes(args,pipe_name=args.job_prefix)
    ssif_jobs=list_jobs(args,job_name=args.job_prefix)

    args.log.info(f'found {len(ssif_pipes)} pipelines to create jobs {sorted(ssif_pipes.keys())}')
    args.log.info(f'found {len(ssif_jobs)} ssif jobs')

    # NOTE: source strategy pipeline for extract defined in load_file csv
    if args.addinf:
        build_mapper_jobs(args,ssif_pipes,ssif_jobs)
        build_reduce_jobs(args,ssif_pipes,ssif_jobs)
        build_pub_jobs(args,ssif_pipes,ssif_jobs)

    #build_extract_jobs_in_batches(args,ssif_pipes,ssif_jobs,batch_size=1,max_wait=100)
    build_all_extract_jobs(args,ssif_pipes,ssif_jobs)


    args.log.info('done')

if __name__=='__main__':
    main()

'''
command line:
python run_generate_jobs.py --srcdir /test/vertica_me/pda1/pcmp --load_version example

python run_generate_jobs.py --src /test/pda1/pcmp/2  -aadinf
python run_generate_jobs.py --src /test/pda1/pcmp/2

snowflake:
show stages;
describe stage  pda1.pcmp.streamstage;
list @pda1.pcmp.streamstage;
rm @pda1.pcmp.streamstage pattern='.*sdc.*';


TODO 
    - code bulk/not-bulk
    - jdbc db vs table db
    - cannot delete job that has an offset

DONE
    - set permissions so etl can see
    - controlhub retry
    - specify dc tags
    - stop all jobs running with same name
    - remove duplicate ssif pipeline filters
    - switch to csv file defining jobs
    - added load_type per table
    - split into bwcrun,bwclib,bwcsetup

    # try:
    #     topology = args.control_hub.topologies.get(topology_name=args.topology_name)
    #     args.control_hub.delete_topology(topology)
    # except:
    #     pass



        'ssif_cdc': {
                "meta_stream_type": "cdc",
                "log_session_window": str(4),
                "meta_target_database_name": args.meta_target_database_name,
                "meta_source_database_connection_string": args.jdbc,
                "meta_source_oracle_pdb": "mypdb",
                "fs_ba_01_file_close_interval": str(150),
                "fs_ba_01_meta_partitions": str(args.partnum),            
                "fs_ba_01_meta_mount_volume": f"{args.dcpath}",           
                "fs_ba_01_meta_mount_bsa": "level",
                "fs_ba_01_meta_env_type": "t"        
            },

'''
