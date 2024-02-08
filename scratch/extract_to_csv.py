"""
GENERATE SQL for view creation.  Use -d or -d and -s for database/schema
to generate SQL for the entire database or a single schema,
The assumed target with be a DATABASE with the same name + _VWS
"""

import argparse, datetime, time, pandas as pd, sys, os 
from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent
lclsetup = root/f"lclsetup"
sys.path.append(root)
sys.path.append(lclsetup)

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup, inf 


def process_args( ):
    parser = argparse.ArgumentParser( description="Convert csv to sources.yml")
    # Requires at least the connection
    parser.add_argument( '--con', help='Database Connection to use (env/key)', required= True)

    # Optional arguments
    parser.add_argument( '--database', help='Source Table Catalog Name', required= False)
    parser.add_argument( '--schema', help='Source Table Schema Name', required= False)
    parser.add_argument( '--table', help='Source Table Name', required= False)
    parser.add_argument( '--out', help='Source to output as db.schema.table convention', required= False)
    parser.add_argument( '--stg', help='fully qualified stage (e.g. @PLAYGROUND.ES_BASE.MOEN )', required= False)


    # Example con: dev/fbronze, src: PLAYGROUND.ES_BASE.AP_AP_SUPPLIERS
    # Example individual:  -d PLAYGROUND -s ES_BASE -t AP_AP_SUPPLIERS
    args = parser.parse_args()

    #  dev/fbronze/ <- env/key
    args.env = args.con.strip('/')
    if args.env.count('/') == 1:
        args.src_env, args.src = args.env.split('/')
    else:
        Exception("Not enough arguments for connection. Use ENV/KEY convention (dev/fbronze)")

    if args.out:
        #  dev/fbronze/INC_DEV_SOURCE <- env/key/DATABASE
        args.out=args.out.strip('/')
        if args.out.count('.') == 2:
            args.database, args.schema, args.table = args.out.split('.')
        else:
            Exception("Not enough arguments in SRC. Use db.schema.table convention")

    return args


def get_col_list( args, src_pkg ):
    # Establish a connection to SF.  Hardcoded for now
    database, schema, table  = args.database, args.schema, args.table
    print(f'=== Fetching Column List for {database}.{schema}.{table} ===')

    inf.run_sql( (src_pkg, 'use secondary roles all') )
    outputfile = f'{database}_{schema}_cols.csv'

    sql = f"""
        with cols_agg as 
            (select 
                table_catalog, table_schema, table_name, 
                LISTAGG(
                case 
                    when data_type in ('TEXT','STRING','VARCHAR') THEN
                    'replace( replace( ' || column_name || ', \\'"\\', \\'\\'), char(10), \\'\\') as ' || column_name
                else
                    column_name
                end, ', ') as col_list
            FROM {database}.information_schema.columns
            where table_catalog = upper( '{database}' )
            and table_schema = UPPER( '{schema}' )
            and table_name   = UPPER(  '{table}' )
            group by table_catalog, table_schema, table_name
            )
        select col_list from cols_agg
        """
    
    print(f"\t::: Generating Column Lists for schema.table: {schema}.{table} :::")

    all_args = (src_pkg, sql)
    col_list = inf.run_sql( all_args )

    print(f"====> Table column list generated.  <=====")
    return col_list


def copy_into_stage( args, src_pkg, col_list ):
    table_catalog, table_schema, table_name  = args.database, args.schema, args.table
    print(f'=== Extracting {table_catalog}.{table_schema}.{table_name} to STAGING ===')
    ck_stage = args.stg if args.stg else "DATA_GOVERNANCE.PUBLIC.UNLOAD_TO_AZURE"

    #PREP PUT ENV
    inf.run_sql( (src_pkg, 'use secondary roles all') )
    if args.stg:
        inf.run_sql( (src_pkg, f'create stage if not exists {ck_stage} ' ))


    print(f"\n====> COPY INTO STAGING: @{ck_stage}  <=====" )
    copy_sql = f""" COPY INTO @{ck_stage}/{table_name}.csv FROM (select {col_list} 
    from {table_catalog}.{table_schema}.{table_name} ) 
    FILE_FORMAT= (TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' COMPRESSION=NONE) SINGLE=TRUE header=true """
    print(f":: USING: {copy_sql}")

    all_args = (src_pkg, copy_sql )
    copy_results = inf.run_sql( all_args )

    print(f"====> COPY INTO @my_stage  <=====\n{copy_results}")


def main():
    args = process_args( )
    col_list =  []
    config = dbsetup.config[0]
    src_pkg = {'config':config, 'env':args.src_env, 'db':args.src, 'schema':args.schema, 'database':args.database } 
    src_db = args.database

    col_list = get_col_list( args, src_pkg )
    if not col_list:
        print(f"## No tables found in the Connection specified."); exit()
    else:
        # pass
        print(f"--- QUERY for column list extract:")
        cols = ''.join( col_list[1][0] ).replace("\\","")
        print('-'*80, '\n', col_list[0], "\n", '-'*80, "\n" )
        # print('-'*80, '\n', col_list[0], "\n", '-'*80, "\n", cols )
        copy_into_stage( args, src_pkg, cols )

if __name__ == "__main__":
    main()

"""
python extract_to_csv.py --con dev/fbronze/ --d PLAYGROUND --s ES_BASE --t AP_AP_SUPPLIERS
python extract_to_csv.py --con dev/fbronze/ --out PLAYGROUND.ES_BASE.APPLSYS_FND_CURRENCIES_TL
python extract_to_csv.py --con dev/fbronze/ --out PLAYGROUND.ES_BASE.APPLSYS_FND_CURRENCIES_TL --stg PLAYGROUND.ES_BASE.MOEN

"""
