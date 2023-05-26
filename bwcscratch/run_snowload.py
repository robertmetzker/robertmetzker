
import sys,os,gzip,csv,argparse,multiprocessing,logging,time,datetime
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

from bwcenv.bwclib import dblib,inf,extract_info
from bwcsetup import dbsetup

def list_stage(con,stagedir,prefix='@~'):
    '''
        https://docs.snowflake.com/en/user-guide/querying-stage.html

        SELECT    
            METADATA$FILENAME AS FILE_NAME, 
            MAX(METADATA$FILE_ROW_NUMBER)  AS NUM_OF_ROWS
        FROM   @~
            WHERE  contains(FILE_NAME, 'table.csv'  )
            GROUP BY METADATA$FILENAME;

    Snowflake automatically generates metadata for files in internal (i.e. Snowflake) stages
    METADATA$FILENAME
        Name of the staged data file the current row belongs to. Includes the path to the data file in the stage.

    METADATA$FILE_ROW_NUMBER
        Row number for each record in the container staged data file.

    '''
    sql=f'''SELECT    
        METADATA$FILENAME AS FILE_NAME, 
        MAX(METADATA$FILE_ROW_NUMBER)  AS NUM_OF_ROWS
        FROM   {prefix}/{stagedir}
        GROUP BY METADATA$FILENAME;
    '''

    #list_files='''list @~;'''
    stage_files=[row for  row in con.fetchdict(sql) ]
    return stage_files

def get_stagedir(dbname,schema,table):
    '''
    https://docs.snowflake.com/en/user-guide/data-load-local-file-system-stage.html
    @~ character combination identifies a user stage, @% character combination identifies a table stage.
    examples: '@~/path 1/file 1.csv'   '@%my table/path 1/file 1.csv'  '@my stage/path 1/file 1.csv

    bwc example: @~/DBTEST/X10057301/test/
    '''
    stagedir=f"{dbname}/{schema}/"+str(table)+'/'
    return stagedir

def exists_stage(con,stagedir):
    '''
        {'FILE_NAME': '@~/NTWK.csv.gz', 'NUM_OF_ROWS': 328}
    '''
    for row in list_stage(con,stagedir):
        if row['FILE_NAME']:
            result=('STOPPING:STAGE:found',row['FILE_NAME'],'rows:',row[ 'NUM_OF_ROWS'])
            return result
    return False

def remove_stage(con,stagedir):
    '''
        {'FILE_NAME': '@~/NTWK.csv.gz', 'NUM_OF_ROWS': 328}
    '''
    remove=0
    for row in list_stage(con,stagedir):
        remove_cmd=f"""rm {row['FILE_NAME']}; """
        con.exe(remove_cmd)
        remove+=1
    return remove

def put(con,path,stagedir):
    '''
        requires the full path
    put file://I:\IT\ETL\nielsenjf\snowflake\extracts_active\ADR_TYP_INFSPLIT_2700000.gz @~/DBTEST/X10057301/ADR_TYP/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';
    '''

    stage_cmd=f'''put file://{path} @~/{stagedir} auto_compress=true;'''
    print(stage_cmd)
    result=list(con.exe(stage_cmd))[0]
    if result['status'] not in  ('UPLOADED'):
        raise Warning(f' no files to load??? {result}')
    print(result)

def create_table(con,dbname,schema,table):
    columns_str='''
        cola TEXT,
        colb TEXT
    '''
    sql=f"""CREATE OR REPLACE  TABLE {dbname}.{schema}.{table} ({columns_str} );\n"""
    print(sql)
    result=list(con.exe(sql))[0]
    if 'successfully created' not in result['status']:
        raise Warning(f' did not create table {result}')

def copy_into(con,dbname,schema,table,stagedir):
    file_format="""file_format =  (type = csv field_delimiter = '\\t' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  on_error='continue'"""
    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""

    copy_cmd=f"""copy into {dbname}.{schema}.{table} from @~/{stagedir} {file_format} on_error='continue'; """
    print(copy_cmd,'\n')
    result=list(con.exe(copy_cmd))[0]
    if result['status'] not in  ('LOADED','SKIPPED'):
        raise Warning(f' no files to load??? {result}')
    print(result)

def create_file(basedir):
    fname=str(basedir/'c.txt')
    data='cola\tcolb\nval1\tval2\nval3\t"val4\tstuff"\n';print(data)
    fw=open(fname,'w');fw.write(data);fw.close()
    return fname

def main():
    #source file
    basedir=Path(os.environ['USERPROFILE'])

    fname=create_file(basedir)
    print('-'*20)
    #target, a "directory" for the table is used instead of simply a file
    # this allows for loading many files in parallel into a table
    dbname='DBTEST';schema='X10057301';table='test'
    stagedir=get_stagedir(dbname,schema,table)

    dbdict = dbsetup.Envs['dev']['snow_me']
    con=dblib.DB(dbdict,port=dbdict.get('port',''))

    #put(con,fname,stagedir)
    rows=list_stage(con,stagedir=stagedir)
    for row in rows: print(rows)
    print('-'*20)
    if exists_stage(con,stagedir):
        result=remove_stage(con,stagedir)
        print('removed',result,'files')

    if exists_stage(con,stagedir):
        print('stage is not EMPTY')
    else:
        print('stage is EMPTY')

    print('-'*20)
    put(con,fname,stagedir)
    print('-'*20)
    create_table(con,dbname,schema,table)
    print('-'*20)
    copy_into(con,dbname,schema,table,stagedir)


if __name__=='__main__':
    #multiprocessing.log_to_stderr(logging.DEBUG)
    main()
    
r'''
Example sequence:
File: c.txt
--------------
cola    colb
val1    val2
val3    "val4\stuff"

CREATE OR REPLACE  TABLE X10057301.test (
        cola TEXT,
        colb TEXT
     );

put file://C:\Users\nielsenjf\c.txt @~/DBTEST/X10057301/test/ auto_compress=true;


copy into X10057301.test from @~/DBTEST/X10057301/test/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  on_error='continue' on_error='continue';


'''