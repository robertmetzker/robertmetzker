import struct, sys, os, argparse, gzip
from pathlib import Path

localdir = Path(f"{os.environ['USERPROFILE']}")/'bwcroot'
sys.path.append(str(localdir))

from bwcenv.bwclib import inf

def process_args():

    etldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    fwdir = f"dev/mf/"
    # DATA:   I:\Data_Lake\CAM
    # LAYOUTS I:\Data_Lake\IT\ETL\a85275\EL\dev\mf\cam_layouts\current_layouts
    datadir = "I:/Data_Lake/CAM"
    layoutdir = etldir + fwdir + "cam_layouts/current_layouts"

    try:
        parser = argparse.ArgumentParser(
            description='command line args', epilog="Example:python extract.py --env uat2 --db db2pt --schema dbmoit00", add_help=True)
        # required
        # No required since these should be in a fixed location

        # optional
        parser.add_argument('--etldir', required=False, default=etldir, help='default directory for logging, data files, etc')
        parser.add_argument('--load_key', required=False, help='YYYY_MM_DD directory under I:/Data_Lake/CAM/')

        # will remove once replaced by iteration through source directory
        parser.add_argument('--noargs', default=False, help='placeholder for jobs not requiring arguments')

        args = parser.parse_args()
    except:
        print(f'Example: python {sys.argv[0]} --load_key 2021_07_03')
        return None

    args.logdir = Path(args.etldir + fwdir)/'cam_layouts/logs'
    args.layoutdir = Path(layoutdir)
    args.datadir = Path(datadir)


    if not args.load_key:
        # get latest directory
        print(f'looking for load_key in:{args.datadir}')
        args.load_key = sorted(
            adir for adir in args.datadir.iterdir() if adir.name.startswith('2'))[-1].stem

    # source dirs
    args.src_data = args.datadir/args.load_key
    args.src_log  = args.layoutdir/'logs'

    # output/target dirs
    args.tgtdir = args.src_data / 'output'
    args.tgtdir.mkdir(parents=True, exist_ok=True)

    # args.datadir=args.etldir/args.srcdir/'data'
    args.log = inf.setup_log(args.logdir, app='parent')
    args.log.info( f'<= processing in:{args.etldir}')
    args.log.debug(f' ::: the environment:{inf.getenv()}')
    args.log.info( f' ::: Determined the load_key: {args.load_key}')



    # args.datadir.mkdir(parents=True,exist_ok=True)
    args.errcnt = 0
    return args


def write_header(args, header, srcfile, mask):
    datafile = Path(str(args.src_data)+'/'+str(srcfile.name))
    args.log.debug(f'DATAFILE>>>> {datafile}')

    tgtfile = Path(str(args.tgtdir)+'/' + str(srcfile.name) + '.csv.gz')
    tmpfile = Path(str(tgtfile) + '.tmp')
    header_str = '\t'.join(header)+'\n'

    with gzip.open(tmpfile, 'wt', compresslevel=1) as fw, open(datafile) as fr:
        args.log.debug(f'  WRITING HEADER: {header} to {tmpfile}')
        fw.write(header_str)

        args.log.debug(f'    WRITING BODY: {srcfile} to {tmpfile}\n')
        for row in fr:
            try:
                row = row.encode()
                cols = struct.Struct(mask).unpack_from(row)
                newcols = [col.decode('ascii').replace('\t', '') for col in cols]
                fw.write('\t'.join(newcols)+'\n')
            except:
                args.log.critical(f' #### ERROR in {srcfile} data: {row}')
                args.log.info(f'--- Issues but moving on...')
                pass
            
    tmpfile.replace(tgtfile)


def fw_unmask(args, srcfile, maskfile):
    args.log.debug(f' >>>> Converting FixedWidth {srcfile} using {maskfile}')
    # Read in the Maskfile-  First line is structure, second line are column names
    wholemask = maskfile.read_text()
    mask = wholemask.split()[0]
    columns = wholemask.split('\n')[1]

    # Split columns and then remove potential empties due to spacing
    c = columns.split()
    tmp = [item.replace(',', '') for item in c]
    header = [i for i in tmp if i]

    write_header(args, header, srcfile, mask)


def main():
    args = process_args()
    if not args:
        return 1
    srcdir = args.layoutdir

    filelist = [afile for afile in sorted(Path(srcdir).glob('layout_*.txt'))]
    shortfn = [i.name for i in filelist]
    args.log.info(
        f' ::: List files for the directory: {srcdir.parent}\n{shortfn}')

    for idx, maskfile in enumerate(filelist):
        ttl = len(filelist)
        pctdone = (idx+1)/ttl*100
        pctviz = '=' * int(pctdone/10)

        srcfile = Path(str(maskfile).replace('layout_', ''))
        datafile = Path(str(args.src_data)+'/'+str(srcfile.name))
        args.log.info(f'>>>> {pctdone:>7.2f}% |{pctviz:<10}| {srcfile.name}')

        print(f'>Layout Found    : {maskfile}')
        print(f'>Passing Datafile: {datafile}')
        if os.path.isfile(datafile):
            args.log.debug(
                f'>>>> Attempting to convert {datafile} using {maskfile}')

            fw_unmask(args, srcfile, maskfile)
        else:
            args.log.debug(f' #### ERROR : {datafile} does not exist')
            args.log.warning(f'--- Skipping: {datafile}')
            args.errcnt += 1

    if args.errcnt > 0:
        args.log.info(
            f' -=# DONE Processing the {ttl} files with {args.errcnt} Error(s) Detected  #=- ')
    else:
        args.log.info(f' === DONE Processing the {ttl} files === ')

    args.log.info(f' === FINAL OUTPUTS CAN BE FOUND IN: {args.tgtdir}')


if __name__ == '__main__':
    main()
