"""
GENERATE SQL for view creation.  Use -d or -d and -s for database/schema
to generate SQL for the entire database or a single schema,
The assumed target with be a DATABASE with the same name + _VWS
"""

import argparse, datetime, time, pandas as pd, re, sys, os 
from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent                  # Back from Run folder to Root DEVOPS_INF
lclsetup = root/f"lclsetup"
sys.path.append(str(root))
sys.path.append(str(lclsetup))

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup ,inf 



def process_args( ):
    parser = argparse.ArgumentParser( description="Generate SQL for view creation.  Use -d or -d and -s for database/schema to generate SQL for the entire database or a single schema, The assumed target with be a DATABASE with the same name + _VWS")
    parser.add_argument( '--src', required=True, help='Source env/key/database (from dbsetup)n e.g. dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS')
    parser.add_argument( '--tgt', help='Target Database Name env/key/database (from dbsetup), e.g. dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS', required= False)

    parser.add_argument( '--from_schema', help='Source Schema Name, e.g. EMTEK_EBS_SB_', required= False)
    parser.add_argument( '--to_schema', help='Target Schema Name, e.g. EMTK_EBS_DEV_', required= False)

    parser.add_argument( '--new', action='store_true', help='Generate new views for the target database')
    parser.add_argument( '--redirect', action='store_true', help='Redirect source views to target database')
    parser.add_argument( '--inc', default=False, action='store_true', help='Flags views to be built as incremental')

    args = parser.parse_args()
    # Default file for testing

    if args.src:
        #  dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS <- env/key/DATABASE/SCHEMA
        args.src=args.src.strip('/')
        if args.src.count('/') == 3:
            args.srcenv, args.srckey, args.src_db, args.src_schema = args.src.split('/')
        else:
            Exception("Not enough arguments in SRC connection.  Need env/key/database/schema where the schema is, e.g. dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS ")

    if args.tgt:
        args.tgt=args.tgt.strip('/')
        if args.tgt.count('/') == 3:
            args.tgtenv, args.tgtkey, args.tgt_db, args.tgt_schema = args.tgt.split('/')
            args.tgt_db = re.sub('_VWS$', '', args.tgt_db.upper())
            args.tgt_db = args.tgt_db.rstrip('_')
            print(f" -----------tgt_db {args.tgt_db}")
        else:
            # args.tgt_db = re.sub('_VWS$', '', args.src_db.upper())
            Exception("Not enough arguments in TGT connection.  Need env/key/database, e.g. dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS")

    if args.from_schema:
        args.from_schema = args.from_schema.upper()
    if args.to_schema:
        from_schema = args.from_schema

    return args

def get_arg_db_info(args, db_type):
    if db_type == 'src':
        env = args.srcenv
        key = args.srckey
    else:
        env = args.tgtenv
        key = args.tgtkey
    return dbsetup.config['env'][env][key]


def generate_new_views( args, col_list ):
    src_db_name = args.src_db
    tgt_db_name = args.tgt_db if args.tgt_db else args.src_db

    print(f'Processing {src_db_name} to {tgt_db_name}')
    # print(f'COL_LIST: {col_list}')

    with open(f'{src_db_name}_vws.sql', 'w') as sqlfile:
        for row in col_list[1]:
            # print(f'row: {row}')
            schema, table, cols = row
            tgt_schema_name = args.tgt_schema if args.tgt_schema else schema
            line = f"""create or replace view "{tgt_db_name}"."{tgt_schema_name}"."{table}" as select {cols} from "{src_db_name}"."{schema}"."{table}" ;"""
            sqlfile.write( line+"\n" )
    print(f'Wrote to: {os.curdir}/{src_db_name}_vws.sql')


def fetch_view_definitions(srcdb, args):
    inf.run_sql( (srcdb, 'use secondary roles all') )
    src_db, src_schema = args.src_db, args.src_schema

    query = f"""
    SELECT TABLE_NAME, VIEW_DEFINITION FROM {src_db}.INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = '{src_schema}'
      AND VIEW_DEFINITION IS NOT NULL"""
    
    curr_vws  = inf.run_sql( (srcdb, query) )
    curr_vws = curr_vws[1] if curr_vws else {} ## 0 is the query SQL
    print( f"====> {len(list(curr_vws))} views found in {src_db}.{src_schema} <=====" )

    # Create an empty dictionary to hold the view names and their definitions
    curr_vws_dict = {}

    # Loop through each tuple in the list
    for tuple_item in curr_vws:
        key = tuple_item[0]  # The view name is the first element of the tuple
        value = tuple_item[1]  # The view definition is the second element of the tuple
        curr_vws_dict[key] = value  # Assign the view definition to the view name key in the dictionary

    # Output the dictionary to verify it contains the correct data
    # print( f'CURR_VWS: {curr_vws_dict}')
    return curr_vws_dict


def redirect_views(views_dict, args):
    src_db, src_schema, from_schema, tgt_db, tgt_schema, to_schema = args.src, args.src_schema, args.from_schema, args.tgt, args.tgt_schema, args.to_schema
    new_views = {}
    src_db_ref = src_db.split('/')[-2]
    tgt_db_ref = tgt_db.split('/')[-2]
    if src_db_ref != tgt_db_ref:
        print( f'Replacing {src_db_ref} with {tgt_db_ref} in view DATABASE references...')
    if src_schema != tgt_schema:
        print( f'Replacing {src_schema} with {tgt_schema} in view SCHEMA references...')
    if from_schema != to_schema:
        print( f'Replacing {from_schema} references with {to_schema} references in views...')

    for view_name, definition in views_dict.items():

        new_definition = definition.replace(src_db_ref, tgt_db_ref)
        if src_schema and tgt_schema:
            new_definition = new_definition.replace(src_schema, tgt_schema)
        if from_schema and to_schema:
            new_definition = new_definition.replace(from_schema, to_schema)
        new_views[view_name] = new_definition
    
    # Use target database and schema names to construct the directory path
    output_dir = Path(f'./{tgt_db}/{tgt_schema or "default"}')
    output_dir.mkdir(parents=True, exist_ok=True)  # Create the directory if it doesn't exist

    # Define the filename for the SQL script
    # filename = output_dir / f'{tgt_db}.{tgt_schema or "default"}.sql'
    filename = output_dir / 'generated_views.sql'
    
    # Open the file for writing
    with open(filename, 'w') as f:
        for view_name, definition in new_views.items():
            # Write each view definition to the file
            # f.write(f"CREATE OR REPLACE VIEW {tgt_db}.{tgt_schema}.{view_name} AS {definition};\n\n")
            f.write(f"{definition}\n\n")
    
    return filename


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
    if args.tgt:
        tgt_db_name = args.tgt_db 
    else:
        tgt_db_name = args.src_db

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


def get_col_dict( args, srcdb ):
    # Establish a connection to SF.  Hardcoded for now
    db_name = args.src_db
    inf.run_sql( (srcdb, 'use secondary roles all') )

    outputfile = f'{db_name}_cols.csv'

    sql = f"""
    select 
        table_schema,
        table_name,
        listagg( column_name, ', ') within group (order by ordinal_position) as table_cols
        from {db_name}.information_schema.columns
    WHERE 1=1 """
    if args.src_schema:
        sql += f" and table_schema = '{args.src_schema}' "
    else:
        sql+= " and table_schema not in ('PUBLIC','INFORMATION_SCHEMA')"
    sql += " group by 1,2 "
    sql += " order by 1,2 "
    
    print(f'=== Fetching Table Dictionaries for {db_name}')

    # qry_results = conn.execute( sql ).fetchall()
    # print(f"I get {len(qry_results)} rows")

    schema = args.src_schema if args.src_schema else 'ALL SCHEMAS'
    print(f"\t::: Generating Column Lists for schema: {schema} :::")

    all_args = (srcdb, sql)
    print(f'USING ARGS: {all_args}')
    col_list = inf.run_sql( all_args )

    # qry_results = conn.execute( sql )
    # qry_results.fetch_pandas_all().to_csv( outputfile )
    # print(f'=== Wrote to {outputfile} \n')
    print(f"====> {len(list(col_list))} table column lists generated.  <=====")
    # print(f'COL_LIST: {col_list}')
    return col_list


def main():
    args = process_args( )

    if args.new and args.redirect:
        print("Error: Cannot specify both --new and --redirect.")
        sys.exit(1)

    col_list =  []

    srcdb = get_arg_db_info(args,'src')
    src_db = args.src_db

    if args.tgt:
        tgt_db = get_arg_db_info(args,'tgt')
    else:
        tgt_db = src_db

    col_list = get_col_dict( args, srcdb )

    if not col_list:
        print(f"## No tables found in the Connection specified."); exit()
    else:
        pass
        # print(f"--- SAMPLE column extract (Schema, Table, Columns):")
        # for i in range(3):
        #     print('-'*80, '\n', col_list[i], "\n" )

    if args.inc:
        schema_sql = create_missing_schemas( args, srcdb )
        inc_views( args, col_list, schema_sql  )

    # Implement the logic for new views generation
    elif args.new:
        print("Generating new views...")
        # Here you would call a function or implement logic to generate new views
        generate_new_views( args, col_list )

    # Implement the logic for redirecting views
    elif args.redirect:
        print("Redirecting views...")
        # Here you would call a function or implement logic to modify existing views
        views_dict = fetch_view_definitions(srcdb, args )
        script_file = redirect_views(views_dict, args )
        print(f"Redirecting Views script generated: {script_file}")

    else:
        # Defaulting to generating new views (even if no flag is provided)
        generate_new_views( args, col_list )



if __name__ == "__main__":
    main()

"""
# Creates Control Tower tables to views in VWS.CT
# "EDP_BRONZE_DEV_VWS"."CT".xxxx created for all tables in "EDP_GOLD_DEV"."CONTROL_TOWER"

python generate_views.py --src dev/fbronze/EDP_GOLD_DEV --src_schema CONTROL_TOWER --tgt dev/fbronze/EDP_BRONZE_DEV --tgt_schema CT --new
python generate_views.py --src dev/playground/PLAYGROUND --src_schema ROBERT_METZKER  --tgt dev/playground/PG  --tgt_schema RM  --redirect

python generate_views.py --src dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS --from_schema EMTEK_EBS_SB_  --tgt dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS  --to_schema EMTK_EBS_DEV_   --redirect
python generate_views.py --src dev/fbronze/PLAYGROUND/ES_EBS__VIEWS --from_schema ES_EBS_  --tgt dev/fbronze/EDP_BRONZE_DEV/EMTEK_EBS_A_VIEWS  --to_schema EMTK_EBS_DEV_   --redirect

python generate_views.py --src dev/fbronze/EDP_BRONZE_PROD_PREV  --inc

python generate_views.py --src dev/funct/ROLLBACK  --src_schema TT_GPD --inc
"""
