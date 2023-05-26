import csv, sys, os, shutil, argparse,datetime
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

set_libpath()

from bwcenv.bwclib import inf


def read_file(args,thsfile):
    colnames=[]
    lns=""
    num=0

    with open(thsfile, 'r') as f:
        reader = csv.reader(f, dialect='excel-tab')
        for row in reader:
            vals=list(row)
            maxl=len(row)

            for i,x in enumerate(row):
                if not x.strip().startswith('#'):
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
                                args.log.warning(f' ### ERROR: This file Format does not seem supported for {thsfile} ###' )
                else:
                    args.log.warning(f' ---- Skipping row in {thsfile} containing a comment:\n{row} ')
                    args.errcnt +=1

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
    '''
    '''
    files = [afile for afile in sorted(Path(args.srcdata).glob('*.csv'))]
    shortfn = [i.name for i in files]

    args.log.info(f' >>> List files for the directory: {args.srcdata.parent}\n{shortfn}')

    for idx,srcfile in enumerate(files):
        pctdone = (idx+1)/len(files)*100
        pctviz = '='* int(pctdone/10)
        args.log.info( f'>>>> {pctdone:>7.2f}% |{pctviz:<10}| {srcfile.name}')

        colnames,lns=read_file(args,srcfile)
        tgtfile=Path(str(args.tgtdata) +'/struct_'+ str(srcfile.name).replace('.csv','')+'.txt')
        write_file(args,tgtfile,colnames,lns)

def process_args():
    '''

    '''
    eldir=f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
 
    try:
        parser = argparse.ArgumentParser(description='command line args',epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00",add_help=True)
        #required
        parser.add_argument( '--srcdir', required=True, help='data source /dev/mainframe/cam_layouts')
        parser.add_argument( '--tgtdir', required=True, help='data target  /dev/mainframe/cam_struct')

        #optional
        #parser.add_argument( '--load_key', default='',help='use a specific load key instead of most recent')
        parser.add_argument( '--eldir', default=eldir, help='default directory for logging, data files, etc')
        parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )

        args = parser.parse_args()

    except:
        print(inf.geterr())
        print(f'Example: python {sys.argv[0]}  --srcdir /dev/mainframe/cam_layouts --tgtdir /dev/mainframe/cam_struct')
        return None

    #build standard directories
    inf.build_args_paths(args)

    args.log=inf.setup_log(args.tgtlog,app='parent',cleanlog=args.cleanlog)
    args.log.info(f'<= processing in:{args.eldir}')
    #args.log.info(f'the environment:{inf.getenv()}')

    args.errcnt = 0

    return args

def main():
    args=process_args()
    if not args: return 1
    convert_mf(args)
    if args.errcnt >0:
        args.log.info(f' -=# DONE Processing the files with {args.errcnt} Error(s) Detected  #=- ')
    else:
        args.log.info(' === DONE Processing the files === ')
    
    args.log.info(f' === FINAL OUTPUTS CAN BE FOUND IN: {args.tgtdata} ')


if __name__=='__main__':
    main()


'''

python run_fixed2struct.py --srcdir /dev/mainframe/cam_layouts --tgtdir /dev/mainframe/cam_struct
'''