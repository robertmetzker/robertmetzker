import csv, sys, os, argparse, logging
from pathlib import Path

#local libs
localdir=Path(f"{os.environ['USERPROFILE']}")/'bwcroot'
sys.path.append(str(localdir))

from bwcenv.bwclib import inf


def loglevel(lvl):
    switcher={
        0:'INFO',
        1:'WARNING',
        2:'ERROR',
        3:'CRITICAL',
        4:'DEBUG'
    }

    return switcher.get(lvl, 'Invalid Logging Level')


def process_args():
    '''

    '''
    etldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
 
    try:
        parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
        #required
        parser.add_argument( '--srcdir', required=True,help='data source /env/mf/cam_layouts')
        parser.add_argument( '--tgtdir', required=True,help='data source /env/mf/cam')

        #optional
        parser.add_argument( '--etldir', default=etldir,help='default directory for logging, data files, etc')
        parser.add_argument( '--lvl', default=0, help='log levels 0-4: info, warning, error, critical, debug')
        args = parser.parse_args()
    except:
        print(inf.geterr())
        print(f'Example: python {sys.argv[0]} --srcdir /dev/mf/cam_layouts /dev/mf/cam')
        return None

    args.srcdir=Path(args.srcdir.strip('/').strip('\\'))
    args.tgtdir=Path(args.tgtdir.strip('/').strip('\\'))
    args.logdir=args.etldir/args.srcdir/'logs'
    args.datadir=args.etldir/args.srcdir/'data'

    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'<= processing in:{args.etldir}')
    #args.log.info(f'the environment:{inf.getenv()}')

    args.datadir.mkdir(parents=True,exist_ok=True)
    args.tgtdir.mkdir(parents=True,exist_ok=True)

    lvl= loglevel( int(args.lvl) )
    args.lvl = lvl

    return args

def convert_mf(args):
    testdir = args.datadir
    # testdir = Path('I:/Data_Lake/CAM/dev/StartEnds')
    outdir = args.tgtdir

    files=[]

    filelist = sorted(Path(testdir).glob('*.csv'))
    for file in filelist:
        files.append(file.name)
        # Can use file.stem if extensions are to be ignored:  stem + '_layout.out'

    args.log.info(f' >>> List files for the directory: {testdir}\n{files}')
    #Initialize Placeholders
    colnames=[]
    lns=""
    num=0


    """
    TODO:  Ignore row if starts with contains #
    Move Writer to INF calls
        def write_csv(fname,rows,raw=False,delim='\t',term='\n',log=None):
     
    Change the if Exists to something like     args.datadir.mkdir(parents=True,exists_ok=True)
    Should be from the PATH library, not args.

    could use:  for i in range(len(filelist))
    pctdone = (i+1)/len(filelist)*100
    then filelist[i]  to reference
    """
    x=0

    for thsfile in filelist:
        tmpfile =  str(thsfile).replace('.csv', '_layout.tmp')
        outfile =  str(thsfile).replace('.csv', '_layout.out')

        args.log.info(f' >>>> Attempting to convert {thsfile} into {tmpfile}')

        if os.path.exists(tmpfile): os.remove(tmpfile)

        with open(testdir/thsfile, 'r') as f:
            reader = csv.reader(f, dialect='excel-tab')
            for row in reader:
                vals=list(row)
                maxl=len(row)

                for i,x in enumerate(row):
                    if not x.strip().startswith('#'):
                        try:
                            if not x.isnumeric():
                                colnames.append(vals[0])
                            else:
                                if maxl==3:
                                    try:
                                        ln=( max( int(vals[1]), int(vals[2])) - min( int(vals[1]), int(vals[2])) )+ 1
                                        if i+1 == maxl:
                                            lns = lns + f'{ln}s'
                                    except:
                                        pass
                                elif maxl==2:
                                    ln = vals[1]
                                    lns = lns + f'{ln}s'
                                else:
                                    args.log.info(f' ### ERROR: This file Format does not seem supported for {thsfile} ###' )
                        except:
                            args.log.info(f' #### ERROR: There was a problem processing {thsfile} into {tmpfile} ####')
                            msg= inf.geterr()
                            args.log.info(f' ### {msg} ###' )
                    else:
                        args.log.warning(f' ---- Skipping row in {thsfile} containing a comment ---- ')
        # Join with comma + space, nets doublespaces.  No spsace, no breaks between values.
        columns = ", ".join(colnames)
        final = f'{lns}\n{columns}'

        
        with open(tmpfile, 'w', newline='') as tf:
            writer = csv.writer(tf, quoting=csv.QUOTE_NONE, delimiter=' ', escapechar=' ')
            writer.writerow([lns])
            writer.writerow([columns])

        '''

        fname=args.tgtdir/f'{tmpfile}'
        
        inf.write_csv(fname,[lns],raw=True,delim='',term='\n',log=None)
        inf.write_csv(fname,[columns],raw=True,delim='',term='\n',log=None)
        '''     

        if os.path.exists(outfile): os.remove(outfile)
        if os.path.exists(tmpfile): os.rename(tmpfile,outfile)

        old = tmpfile.rpartition('\\')[2]
        new = outfile.rpartition('\\')[2]        
        args.log.info(f'<<<< Renamed {old} => {new}'  )
        args.log.debug(f'Final Results for {new}:\n{final}\n')

        colnames=[]
        columns = ""
        lns= ""


def main():
    args=process_args()
    if not args: return 1
    print('got',args)
    args.log.debug('debug message')
    args.log.info('info message')
    args.log.warning('warn message')
    args.log.error('error message')
    args.log.critical('critical message')

    convert_mf(args)

    args.log.info(' === DONE Processing the files === ')

if __name__=='__main__':
    main()
        