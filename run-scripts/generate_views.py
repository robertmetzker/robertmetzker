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
sys.path.append(str(root))
sys.path.append(str(lclsetup))

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup, inf 


def process_args( ):
    parser = argparse.ArgumentParser( description="Convert csv to sources.yml")
    parser.add_argument( '--src_schema', help='Source Schema Name', required= False)
    parser.add_argument( '-t','--tgt_db', help='Target Database Name', required= False)
    parser.add_argument( '-v','--tgt_schema', help='Target Schema Name', required= False)
    parser.add_argument( '--inc', default=False, action='store_true', help='Flags views to be built as incremental')
    parser.add_argument( '--src', required=True, help='Source env/key/database (from dbsetup)')

    args = parser.parse_args()
    # Default file for testing

    if args.src:
        #  dev/fbronze/INC_DEV_SOURCE <- env/key/DATABASE
        args.src=args.src.strip('/')
        if args.src.count('/') == 2:
            args.src_env, args.src, args.src_db = args.src.split('/')
        else:
            Exception("Not enough arguments in SRC connection.  Need env/key/database, e.g. dev/fbronze/INC_DEV_SOURCE")

    if args.tgt_db:
        args.tgt_db = args.tgt_db.upper().rstrip('VWS').rstrip('_')

    if args.src_schema:
        args.src_schema = args.src_schema.upper()

    return args


def std_views( args, col_list ):
    src_db_name = args.src_db
    tgt_db_name = args.tgt_db if args.tgt_db else args.src_db

    with open(f'{src_db_name}_vws.sql', 'w') as sqlfile:
        for row in col_list:
            schema, table, cols = row
            tgt_schema_name = args.tgt_schema if args.tgt_schema else schema
            line = f"""create or replace view "{tgt_db_name}_VWS"."{tgt_schema_name}"."{table}" as select {cols} from "{src_db_name}"."{schema}"."{table}" ;"""
            sqlfile.write( line+"\n" )

    print(f'Wrote to: {os.curdir}/{src_db_name}_vws.sql')


def inc_views( args, col_list, schema_sql ):
    """
    CREATE OR REPLACE VIEW INC_VIEWS.COLLECTION_PRODUCT as (
    with mx as ( select max( FB_DW_DATE_ADDED ) as max_load_key
                from SHOPIFY_FIBERON.COLLECTION_PRODUCT 
        ),
    tst as (
        select
            case when FB_DW_LAST_SEEN = max_load_key then 'Y' end as CURRENT_IND
            ,case when nvl( CURRENT_IND,'X') != 'Y' then 'D'
                when FB_DW_DATE_ADDED is null or
                    FB_DW_LAST_SEEN > FB_DW_DATE_ADDED then 'U'
                when FB_DW_LAST_SEEN = FB_DW_DATE_ADDED then 'I'
            end as FB_STATUS
            ,*
        from  SHOPIFY_FIBERON.COLLECTION_PRODUCT 
            left join mx on ( FB_DW_LAST_SEEN = max_load_key )
        )
    select 
        * from tst
    );    
    """
    src_db_name = args.src_db
    tgt_db_name = args.tgt_db if args.tgt_db else args.src_db

    print(f"=== Generating INCREMENTAL views for {src_db_name}")

    with open(f'{src_db_name}_inc_vws.sql', 'w') as sqlfile:
        if schema_sql:
            sqlfile.write( "/* GENERATING TARGET VWS_ SCHEMAS */\n" )
            all_schemas = schema_sql[1]
            for sql in all_schemas:
                output = sql[0]
                sqlfile.write( output +"\n" )
        else:
            pass

        parse_col = col_list[1] if len(col_list) ==2 else col_list[0]
        sqlfile.write( "/* GENERATING VIEWS */\n" )
        for row in parse_col:
            schema, table, cols = row
            tgt_schema_name = args.tgt_schema if args.tgt_schema else schema
            line = f"""create or replace view "{tgt_db_name}"."VWS_{schema}"."{table}" as ( with mx as ( select max( FB_DW_DATE_ADDED ) as max_load_key from "{src_db_name}"."{schema}"."{table}" ), tst as ( select case when FB_DW_LAST_SEEN = max_load_key then 'Y' end as CURRENT_IND, case when nvl( CURRENT_IND,'X') != 'Y' then 'D' when FB_DW_DATE_ADDED is null or FB_DW_LAST_SEEN > FB_DW_DATE_ADDED then 'U' when FB_DW_LAST_SEEN = FB_DW_DATE_ADDED then 'I' end as FB_STATUS, * from  "{src_db_name}"."{schema}"."{table}" left join mx on ( FB_DW_LAST_SEEN = max_load_key ) ) select * from tst ); """
            sqlfile.write( line+"\n" )

    print(f'Wrote to: {os.curdir}/{src_db_name}_inc_vws.sql')


def create_missing_schemas( args, src_pkg):
    db_name = args.src_db
    inf.run_sql( (src_pkg, 'use secondary roles all') )
    schemas = []

    sql = f"""
with src_schemas as ( 
    select schema_name from {db_name}.information_schema.schemata 
        where schema_name not in ('PUBLIC','INFORMATION_SCHEMA') and left(schema_name,4) != 'VWS_'
    MINUS
    select right( schema_name, len(schema_name)-4) as schema_name from {db_name}.information_schema.schemata 
        where schema_name not in ('PUBLIC','INFORMATION_SCHEMA') and left(schema_name,4) = 'VWS_'
)
    select 'create schema VWS_'||schema_name ||' ;' as "--SQL"
from src_schemas"""


    print(f'=== Fetching Schema Names for {db_name}')
    all_args = (src_pkg, sql)
    schema_sql = inf.run_sql( all_args )

    print(f"====> {len(list(schema_sql))} schemas need to be generated.  <=====")
    return schema_sql


def get_col_dict( args, src_pkg ):
    # Establish a connection to SF.  Hardcoded for now
    db_name = args.src_db
    inf.run_sql( (src_pkg, 'use secondary roles all') )

    outputfile = f'{db_name}_cols.csv'

    sql = f"""
    select 
        table_schema,
        table_name,
        listagg( column_name, ', ') within group (order by ordinal_position) as table_cols
        from {db_name}.information_schema.columns
    WHERE 1=1 """
    if args.src_schema:
        sql += " and table_schema = '{args.src_schema}' "
    else:
        sql+= " and table_schema not in ('PUBLIC','INFORMATION_SCHEMA')"
    sql += " group by 1,2 "
    sql += " order by 1,2 "
    
    print(f'=== Fetching Table Dictionaries for {db_name}')

    # qry_results = conn.execute( sql ).fetchall()
    # print(f"I get {len(qry_results)} rows")

    schema = args.src_schema if args.src_schema else 'ALL SCHEMAS'
    print(f"\t::: Generating Column Lists for schema: {schema} :::")

    all_args = (src_pkg, sql)
    col_list = inf.run_sql( all_args )

    # qry_results = conn.execute( sql )
    # qry_results.fetch_pandas_all().to_csv( outputfile )
    # print(f'=== Wrote to {outputfile} \n')
    print(f"====> {len(list(col_list))} table column lists generated.  <=====")
    return col_list


def main():
    args = process_args( )
    col_list =  []
    config = dbsetup.config[0]
    src_pkg = {'config':config, 'env':args.src_env, 'db':args.src, 'schema':args.src_schema, 'database':args.src_db } 
    src_db = args.src_db

    col_list = get_col_dict( args, src_pkg )

    if not col_list:
        print(f"## No tables found in the Connection specified."); exit()
    else:
        pass
        # print(f"--- SAMPLE column extract (Schema, Table, Columns):")
        # for i in range(3):
        #     print('-'*80, '\n', col_list[i], "\n" )

    if args.inc:
        schema_sql = create_missing_schemas( args, src_pkg )
        inc_views( args, col_list, schema_sql  )
    else:
        std_views( args, col_list )


if __name__ == "__main__":
    main()

"""

python generate_views.py -d EDP_BRONZE_DEV
python generate_views.py -d EDP_BRONZE_DEV -s CONTROL_TOWER -t PLAYGROUND -v CT
python generate_views.py --src dev/fbronze/INC_DEV_SOURCE --src_schema SHOPIFY_FIBERON --inc

python generate_views.py --src dev/fbronze/EDP_BRONZE_PROD_PREV  --inc

"""