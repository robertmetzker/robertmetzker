#std libs
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime,subprocess,time,re
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

from bwcenv.bwclib import inf

def process_command_line(args,command_line):
    args.log.info(f'line {command_line}')

    fields=command_line.split(';')
    jobtype,sleep,command=fields 
    args.log.debug(f'jobtype:{jobtype} sleep:{sleep} command:{command}')

    sleep=int(sleep)
    if sleep: 
        args.log.debug(f'sleeping for {sleep} seconds')
        time.sleep(int(sleep))

    if '.py' in command:
        pyprog=command[:command.find('.py')+3]
        if '/' not in pyprog and '\\'  not in pyprog:
            command=str(args.rundir)+'/'+command
            args.log.debug(f'adding path {command}')
        command='python '+str(command)
    if '[' in command and ']' in command:
        to_replace=re.findall('\[(.*?)\]',command)
        for astr in to_replace:
            val=getattr(args,astr)
            command=command.replace(f'[{astr}]',val)
        args.log.debug(f'replaced [] {command}')

    if jobtype == 'fg':
        results=inf.run_proc(command,raise_error=False,log=args.log)
    elif jobtype == 'bg':
        results=inf.run_proc(command,raise_error=False,wait=False,log=args.log)
    elif jobtype == 'fgi':
        results=inf.run_proc(command,raise_error=False,log=args.log)
    elif jobtype == 'bgi':
        results=inf.run_proc(command,raise_error=False,wait=False,log=args.log)
    else:
        raise ValueError(f'invalid jobtype: {jobtype}')

    args.log.debug((f"Program results:\nJOB RETURN:{results['return']}\nJOB OUT:\n{results['out']}\nJOB ERROR:\n{results['error']}"))

    #if there is a non zero return value, raise an error
    if jobtype == 'fg':
        if results.get('return'): raise ValueError(f"PROC ERROR\nRETURN:{results['return']}\nOUT:\n{results['out']}\nERROR:\n{results['error']}")

def job_runner(args):
    args.log.info(f'running job {args.job}')
    job_text=Path(args.job).read_text().strip()
    args.log.info(f'contents of job:\n----JOB CONTENTS----\n{job_text}\n----\n')
    errors=[]        

    for command_line in job_text.splitlines():
        command_line=command_line.strip()
        if not command_line: continue
        if command_line.startswith('#'): continue

        args.log.info(f'------------------ at: {command_line}')
        if args.ignore:
            args.log.debug('job processing will continue past errors')
            process_command_line(args,command_line)
        else:
            try:
                args.log.debug('job processing will stop at first error')
                process_command_line(args,command_line)
            except:
                errors.append('ERROR:'+inf.geterr(clean=False))
    if errors:
        error_lines=[]
        for error in errors:
            error_rows=[line for line in error.split('\n') if 'ERROR' in line]
            error_str='\n'.join(error_rows)
        raise ValueError(f'ran full job some (or all) rows failed: error count:{len(errors)}   errors:{error_str}')

def process_args():
    '''

    '''

    jobdir="bwcjobs"
    logdir="logs"
    eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}"
    codedir=str(Path(f"{os.environ['USERPROFILE']}")/'bwcroot')
    try:
        parser = argparse.ArgumentParser(description='command line args',epilog="Example:python run_jobs.py --env dev --job test1 --param1 bwc_mod",add_help=True)
        #required
        parser.add_argument( '--env', required=True,help='environment dev/uat2/prod')
        parser.add_argument( '--job',help='job file to run')
        #boolean
        parser.add_argument( '--ignore', default=True,action='store_false',help='continue even if there is an error')
        #optional
        parser.add_argument( '--param1',help='other arg')
        parser.add_argument( '--param2',help='other arg')
        parser.add_argument( '--param3',help='other arg')
        parser.add_argument( '--logdir', default=logdir,help='dir location for job files')
        parser.add_argument( '--codedir', default=codedir,help='dir location for job files')
        parser.add_argument( '--eldir', default=eldir,help='default directory for logging, data files, etc')
        args = parser.parse_args()
    except:
        print(inf.geterr())
        print(f'Example: python {sys.argv[0]} --env dev --job test1')
        sys.exit(2)

    now=datetime.datetime.now()
    ymd=now.strftime('%Y_%m_%d') #2021_05_14

    #dir setup
    args.codedir=Path(args.codedir)
    args.bwcenvdir=args.codedir/'bwcenv'
    args.rundir=args.bwcenvdir/'bwcrun'
    args.eldir=Path(args.eldir)/'EL'/args.env

    args.logdir=args.eldir/('JOB_'+args.logdir)/ymd
    args.jobdir=args.bwcenvdir/jobdir
    args.job=Path(args.jobdir)/(args.job+'.txt')

    prog=Path(sys.argv[0]).name
    args.log=inf.setup_log(args.logdir,app=f'JOB_'+args.job.stem)
    args.log.info(f'running jobs using {args}')
    #args.log.info(f'the environment:{inf.getenv()}')

    return args

def main():
    args=''
    try:
        args=process_args()
        #args.log.info(f"\n{'-'*120}\n JOB START\n{'-'*120}")
        job_runner(args)
        #args.log.info(f"\n{'-'*120}\n JOB END\n{'-'*120}")
    except:
        if args: args.log.error(f'JOB runner error! {inf.geterr(clean=False)}')
        else: print(inf.geterr(clean=False))
        sys.exit(1)
if __name__=='__main__':
    main()

'''
Notes

Note what happens on i drive with different argument w\args.schemadir/'logs'
    - python run_example2.py --srcdir /dev/vertica_dev1/pcmp
    - python run_example2.py --srcdir /dev/vertica_dev1/pcmp


'''