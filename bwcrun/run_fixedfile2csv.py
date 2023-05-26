import struct, sys, os, argparse, gzip, csv, datetime
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


# #local libs
set_libpath()

from bwcenv.bwclib import inf

#----------------------------------------------------------

def write_file(allargs):
    args, srcfile, header, mask= allargs
    level=1
    args.log=inf.setup_log(args.tgtlog,app=f'child_{srcfile.stem}',prefix=(level+1)*'\t',cleanlog=args.cleanlog)
    datafile = srcfile#Path(str(args.filedir)+'/'+str(srcfile.name))
    args.log.debug(f'DATAFILE>>>> {datafile}')

    tgtfile = args.tgtextractdir/(srcfile.stem + '.csv.gz')
    tmpfile = Path(str(tgtfile) + '.tmp')
    header_str = '\t'.join(header)+'\n'

    # with gzip.open(tmpfile, 'wt', compresslevel=1) as fw, open(datafile,encoding='ascii',errors='ignore') as fr:
    with gzip.open(tmpfile, 'wt', compresslevel=1) as fw, open(datafile) as fr:
        args.log.debug(f'  WRITING HEADER: {header} to {tmpfile}')
        fw.write(header_str)

        args.log.debug(f'    WRITING BODY: {srcfile} to {tmpfile}\n')
        try:
            for row in fr:
                try:
                    row = row.encode()
                    cols = struct.Struct(mask).unpack_from(row)
                    newcols = [col.decode('ascii',errors='ignore').replace('\t', '') for col in cols]
                    fw.write('\t'.join(newcols)+'\n')
                except:
                    args.log.critical(f' #### ERROR in {srcfile} data: {row}')
                    args.log.info(f'--- Issues but moving on...')
                    pass
        except:
            args.log.debug(f'Failed to read {fr} ')

    tmpfile.replace(tgtfile)
    args.log.info(f'wrote {tgtfile}')

def build_metadata_for_loading(args,data_struct):
    '''
    # Structure followed by Column Names
    1s5s50s12s8s8s8s8s8s
    CHANGE_INDICATOR,  MCO_ID_NO,  MCO_NAME,  NTWRK_ID_NO,  API_RCPT_DATE,  TOTAL_SBMTD_QTY,  TOTAL_PASS_QTY,  TOTAL_FAIL_QTY,  DW_CNTRL_DATE

    # Generated DDL from querying the database
    datatype    display_size    internal_size    name    null_ok    precision    scale
    VARCHAR    128    -1    table_name    True        
    VARCHAR    128    -1    PRIMARY_KEY    True        
    VARCHAR    1    -1    ISKEY    True    

    #From running the Calculate Layout against a CSV output file containing Headers
    datatype    display_size    internal_size    name    null_ok    precision    scale
    VARCHAR    1    -1    ISKEY    True        
    VARCHAR    18    -1    PRIMARY_KEY    True        
    VARCHAR    13    -1    table_name    True    

    '''
    wholemask = data_struct.read_text()
    rows=wholemask.split('\n')
    mask = rows[0];columns = rows[1]
    widths = [int(i) for i in mask.split('s') if i]
    col_names  = [i.strip() for i in columns.split(',') if i]

    if len(col_names) != len(widths): raise ValueError(f'col mismatch data_struct {len(col_names)} {len(widths)} ')

    col_formats=[]

    for col_name,width in zip(col_names,widths):
        col_formats.append({'datatype':'VARCHAR', 'display_size':width, 'internal_size':-1, 'name':col_name, 'null_ok':'True' , 'precision':0,'scale':0    })

    return col_names,col_formats,mask

def write_dll_and_convert_file(args,data_file,data_struct):

        #----- metadata for loading
        col_names,ddl_info,mask=build_metadata_for_loading(args,data_struct)
        ddl_file=args.tgtddldir/f'{data_file.stem}.csv'
        inf.write_csv(ddl_file,ddl_info,log=args.log)
        args.log.debug(f'wrote DDL for {data_file.stem} to: {ddl_file}')

        #----- convert to delim data file
        write_file(args,data_file,col_names,mask)

def get_file_structs(args):
    '''
    the data files match the struct layout files names minus the prefix
        WindowsPath('I:/Data_Lake/CAM/2021_07_10/test.txt'): WindowsPath('I:/Data_Lake/IT/ETL/nielsenjf/EL/dev/mainframe/cam_struct/data/struct_test.txt')
    '''

    file_structs={}
    structlist=[]

    args.log.info(f'looking for struct definitions in {args.structdir} with prefix: {args.struct_prefix}')
 
    for structfile in sorted(Path(args.structdir).glob(f'{args.struct_prefix}*.txt')):
        data_filename=Path(args.filedir)/args.load_key/structfile.name.replace(args.struct_prefix, '')
        # srcfile = Path(str(structfile).replace(args.struct_prefix, ''))
        # datafile = Path(str(args.src_data)+'/'+str(srcfile.name))
        file_structs[data_filename]=structfile
        if not data_filename.exists(): 
            # raise ValueError(f'data file {data_filename} is missing')
            args.log.debug(f'data file {data_filename} is missing')
            continue 
    
    return file_structs

# ----------------------------------------  MAINLINE CODE


def process_args():

    eldir = f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL"
    #datadir = "I:/Data_Lake/CAM"

    example=f'python {sys.argv[0]} --filedir I:/Data_Lake/CAM --tgtdir /dev/mainframe/cam  --structdir /dev/mainframe/cam_struct'
    parser = argparse.ArgumentParser(
        description='command line args', epilog=example, add_help=True)
    # required
    parser.add_argument( '--filedir', required=True , help='typically fixed location for data source')
    parser.add_argument( '--tgtdir', required=True, help='tgt dir for data and logs')
    parser.add_argument('--structdir', required=True, help='where struct defs are /dev/mainframe/cam_struct')
    # optional
    parser.add_argument('--eldir', required=False, default=eldir, help='default directory for logging, data files, etc')
    parser.add_argument('--load_key', required=False, help='YYYY_MM_DD directory')
    parser.add_argument('--struct_prefix', default='struct_', help='YYYY_MM_DD directory')
    parser.add_argument( '--cleanlog', action='store_true', help='Changes the writemode of the logger to overwrite' )

    args = parser.parse_args()



    args.filedir=Path(args.filedir)
    #get custom load key
    args.load_key=sorted(adir for adir in args.filedir.iterdir() if adir.name.startswith('2'))[-1].stem

    #builds: args.tgtdata,args.tgtlog,args.srcdir,args.srcdata
    #default: use_load_key=False, find_src_load_key=False 
    inf.build_args_paths(args,use_load_key=True, find_src_load_key=False)

    #custom directories
    args.structdir=Path(args.eldir)/args.structdir.strip('/').strip('\\')/'data'
    args.tgtddldir=args.tgtdata/'ddl'
    args.tgtddldir.mkdir(parents=True,exist_ok=True)
    args.tgtextractdir=args.tgtdata/'extracts'
    args.tgtextractdir.mkdir(parents=True,exist_ok=True)

    #etup loggin
    args.log = inf.setup_log(args.tgtlog, app='parent', prefix='', cleanlog=args.cleanlog)
    args.log.info( f'<= processing in: {args.eldir}')
    #args.log.debug(f' ::: the environment:{inf.getenv()}')
    args.log.info( f' ::: Determined the load_key: {args.load_key}')

    args.errcnt = 0
    return args

def get_file_size(file_path):
    """
       Get the size of the file in bytes
       Determine the number of bytes on the first line
       Divide the size by bytes to determine number of lines 
    """
    file_obj = Path(file_path)
    size = file_obj.stat().st_size
    
    #Returns the longest line
    try:
        maxlen = len( max(open(file_obj,'r'), key=len))  + 1  
    except:
        print(f'#### ERR: Unable to determine the width of {file_obj}')
        maxlen = -1
        pass

    return size, maxlen


def main():
    args = process_args()

    if not args:
        return 1

    file_struct_dict=get_file_structs(args)
    all_args=[]


    args.log.debug(f'\n==== PROCESSING: {file_struct_dict.items()}')
    '''
    Output cludgy csv file with flatfile calculated line counts...
    '''
    ffheader=['data_file','bytes','fixed_width','total_lines']
    fw_summary=[]

    for data_file,data_struct in file_struct_dict.items():
        size, maxlen = get_file_size(data_file)        
        lines = size/maxlen
        fw_line=(data_file.stem, size, maxlen, lines)

        #----- metadata for use in loading later
        col_names,ddl_info,mask=build_metadata_for_loading(args,data_struct)
        ddl_file=args.tgtddldir/f'{data_file.stem}.csv'

        inf.write_csv(ddl_file,ddl_info,log=args.log)
        args.log.info(f'wrote DDL for {data_file.stem} to: {ddl_file}')

        if maxlen > -1: 
            args.log.info(f'          {data_file.stem} has {lines:,} lines at {maxlen} each, for a total of {size} bytes')
            fw_summary.append( fw_line )
        else: 
            args.log.warning(f'#### ERR: Unable to determine the width of {data_file.stem}')

        #----- convert to delim data file
        func_args=(args,data_file,col_names,mask)
        #write_file(func_args)
        all_args.append(func_args)

    results=inf.run_parallel(write_file,all_args,parallel=5,log=args.log)

    # inf.write_csv(report,results,log=args.log,delim=',')
    args.log.info(f' ===! Output Results written to {args.tgtlog}/fw_linecounts.csv !===')
    
    with open(args.tgtlog/'fw_linecounts.csv','w', newline='' ) as sc:
        writer = csv.writer(sc)
        writer.writerow(ffheader)
        writer.writerows(fw_summary)

if __name__ == '__main__':
    main()


'''
python run_fixed2struct.py --srcdir /dev/mainframe/cam_layouts --tgtdir /dev/mainframe/cam_struct
python run_fixedfile2csv.py --filedir I:/Data_Lake/CAM --tgtdir /dev/mainframe/cam  --structdir /dev/mainframe/cam_struct


'''