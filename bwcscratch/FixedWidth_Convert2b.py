import csv, sys, os, argparse
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
        #print(inf.geterr())
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
    args.errcnt = 0

    return args

def read_file(args,thsfile):
    colnames=[]
    lns=""
    num=0

    with open(thsfile, 'r') as f:
        reader = csv.reader(f, dialect='excel-tab')
        for row in reader:
            vals=list(row)
            maxl=len(row)

            tst={}

            for i,x in enumerate(row):
                if not x.strip().startswith('#'):
                        if not x.isnumeric():
                            tst[ thsfile.name ]={'datatype':'VARCHAR', 'display_size':0, 'internal_size':-1, 'name':vals[0], 'null_ok':'True' , 'precision':0, 'scale':0   }
                            colnames.append(vals[0])
                        else:
                            if maxl==3:
                                try:
                                    ln=( max( int(vals[1]), int(vals[2])) - min( int(vals[1]), int(vals[2])) )+ 1
                                    if i+1 == maxl:
                                        lns = lns + f'{ln}s'
                                        tst[i]={'display_size':int(lns)}
                                except:
                                    pass
                            elif maxl==2:
                                ln = vals[1]
                                lns = lns + f'{ln}s'
                                tst[i]={'display_size':int(lns)}
                            else:
                                args.log.warning(f' ### ERROR: This file Format does not seem supported for {thsfile} ###' )
                else:
                    args.log.warning(f' ---- Skipping row in {thsfile} containing a comment:\n{row} ')
                    args.errcnt +=1

            with open(tstda,'w') as wt:
                while
                wt.write(header_str)

            print(f'COMBINED: {tst}')

    return colnames,lns


def write_file(args,tgtfile,colnames,lns):
    tmpfile=Path( str(tgtfile)+'.tmp')
    columns = ", ".join(colnames)
    
    
    with open(tmpfile, 'w', newline='') as tf:
        writer = csv.writer(tf, quoting=csv.QUOTE_NONE, delimiter=' ', escapechar=' ')
        writer.writerow([lns])
        writer.writerow([columns])

    tmpfile.replace(tgtfile)

    final = f'{lns}\n{columns}'
    args.log.debug(f'<<<< Renamed {tmpfile.name} => {tgtfile.name}'  )
    args.log.debug(f'Final Results for {tgtfile.name}:\n{final}\n')

def convert_mf(args):
    testdir = args.datadir
    # testdir = Path('I:/Data_Lake/CAM/dev/StartEnds')
    outdir = args.tgtdir

    files = [afile for afile in sorted(Path(testdir).glob('*.csv'))]
    shortfn = [i.name for i in files]

    args.log.info(f' >>> List files for the directory: {testdir.parent}\n{shortfn}')

    for idx,srcfile in enumerate(files):
        pctdone = (idx+1)/len(files)*100
        pctviz = '='* int(pctdone/10)
        args.log.info( f'>>>> {pctdone:>7.2f}% |{pctviz:<10}| {srcfile.name}')

        colnames,lns=read_file(args,srcfile)
        tgtfile=Path(str(srcfile.parent) +'/layout_'+ str(srcfile.name).replace('.csv','')+'.txt')
        write_file(args,tgtfile,colnames,lns)


def main():
    args=process_args()
    if not args: return 1
    convert_mf(args)
    if args.errcnt >0:
        args.log.info(f' -=# DONE Processing the files with {args.errcnt} Error(s) Detected  #=- ')
    else:
        args.log.info(' === DONE Processing the files === ')
    
    args.log.info(f' === FINAL OUTPUTS CAN BE FOUND IN: {args.datadir} ')


if __name__=='__main__':
    main()
        