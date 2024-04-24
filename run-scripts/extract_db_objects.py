import os

import snowflake.connector
import argparse, os, re, sys, time, pandas as pd
from platform import python_version
from pathlib import Path

prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent
lclsetup = root/f"lclsetup"
sys.path.append(str(root))
sys.path.append(str(lclsetup))
from lclsetup import dbsetup, inf 


def process_args( ):
    parser = argparse.ArgumentParser( description="Convert csv to sources.yml")
    parser.add_argument( '--src', required=True, help='Source env/key/database (from dbsetup)')
    parser.add_argument( '-s','--schema', help='Partial Schema name to filter on', required= False)
    parser.add_argument( '-o','--output', help='Output folder to store the extracted information', required= False)

    args = parser.parse_args()

    if args.src:
        #  dev/fbronze/INC_DEV_SOURCE <- env/key/DATABASE
        args.src=args.src.strip('/')
        if args.src.count('/') == 2:
            args.srcenv, args.srckey, args.src_db = args.src.split('/')
        elif args.src.count('/') == 1:
            args.srcenv, args.srckey = args.src.split('/')
            args.src_db = None
        else:
            Exception("Not enough arguments in SRC connection.  Need env/key/database, e.g. dev/fbronze/INC_DEV_SOURCE")

    return args

def get_arg_db_info(args, db_type):
    if db_type == 'src':
        env = args.srcenv
        key = args.srckey
    else:
        env = args.tgtenv
        key = args.tgtkey
    return dbsetup.config['env'][env][key]

def get_databases( cursor, database ):
    sql = f"""select database_name from information_schema.databases """
    if database:
        sql += f""" where database_name like '{database}%' """
    sql += """ order by 1  """
    cursor.execute( sql )
    return cursor.fetchall()

def get_schemas( cursor, db_name, schema ):
    sql = f"""select schema_name from {db_name}.information_schema.schemata """
    if schema:
        sql += f""" where schema_name like '{schema}%' """
    sql += """ order by 1  """
    cursor.execute( sql )
    return cursor.fetchall()

def get_ftype_ddl( cursor, db_name, schema, ftype, proc_name ):
    cursor.execute( 'use secondary roles all' )
    proc_ddl =''
    sql = f"""select {ftype}_name, argument_signature from {db_name}.information_schema.{ftype}s """
    if schema:
        sql += f""" where {ftype}_schema like '{schema}%' """
    sql += f""" and {ftype}_name = '{proc_name}' """
    sql += f""" qualify row_number() over( order by len(argument_signature) desc  ) = 1"""
    cursor.execute( sql )
    procs =  cursor.fetchall()
    # print(f"\t...> {sql}")

    # with argument_signature of: (TABLE_CATALOG VARCHAR, TABLE_SCHEMA VARCHAR, TABLE_NAME VARCHAR), extract:(VARCHAR, VARCHAR, VARCHAR)
    if procs:
        # print(procs)
        for proc in procs:
            proc_name, proc_args = proc[0], proc[1]
            if proc_args == '()':
                arg_list = ''
            else:
                ddlargs = re.search(r'\((.*)\)', proc_args).group(1)
                # Split arguments into list
                arg_list = ddlargs.split(', ')
                # Parse each argument into name and datatype
                arg_dtypes = []
                for arg in arg_list:
                    name, dtype = arg.split(' ')
                    arg_dtypes.append((name, dtype))

                arg_list = ', '.join( [x[1] for x in arg_dtypes] ) 

            extract_sql = f"""select get_ddl('{ftype}', '{db_name}.{schema}.{proc_name}({arg_list})') as ddl"""
            # print(f"\t...> {extract_sql}")
            cursor.execute( extract_sql )
            proc_ddl =  cursor.fetchall()
        
            return proc_ddl[0][0]
            # print(f">>\n\n{extract_sql}:: \n{proc_ddl[0][0][:80]}\n\n<<<")
    else:
        print(f"""!!! Problem with {ftype} {proc_name}:\n{proc}""")


def main():
    args= process_args()

    srcdb = get_arg_db_info(args,'src')
    database = args.src_db 
    schema = args.schema
    output_dir = args.output if args.output else 'extracted'

    # Make an initial connection to use to grab information about the database.
    cursor = inf.get_dbcon( srcdb )
    cursor.execute(f"use secondary roles all")

    object_type_dict = {
        # 'file_format', 'pipes', 'external_tables'
        'tables': 'table_name',
        'views': 'table_name',
        'functions': 'function_name',
        'procedures': 'procedure_name',
        'stages': 'stage_name',
    }

    # TODO:  If no database was given on CLI, use get_databases to get a list of databases to loop over.
    # Create this as a list of dictionaries that will have a database name and a list of schema names.
    # e.g. database_structure: {'database_name': {'schema_name': ['table1','table2'], 'schema2':['table1']... } }
    database_structure = {}
    database_list = []

    if not args.src_db:
        print(f">>>>> Fetching DATABASE names")
        databases = get_databases( cursor, database )
        database_list = [db[0] for db in databases ]
        print(f">>> Found {len(database_list)} databases")
        print( database_list )
    else:
        database_list = [database]
        print(f">>>> Using DATABASE {database}")

    # Loop through the databases and get the schema names.
    for db_name in database_list:
        print(f">>> Fetching Schema names for {db_name}")
        if not args.schema:
            schemas = get_schemas( cursor, db_name, None )
            schema_list = [schema[0] for schema in schemas ]
            print(f">>> Found {len(schema_list)} schemas")
        else:
            schema_list = [schema]
            print(f">>> Using SCHEMA {schema}")

        # Loop through the schemas and get the object names, and append them to the database_structure
        for schema in schema_list:
            print(f">>> Fetching Objects for {db_name}.{schema}")
            for object_type, object_name in object_type_dict.items():
                obj_schema = 'table' if object_type == 'views' else object_type.rstrip('s') 
                sql = f"""
                select {object_name}
                from {db_name}.INFORMATION_SCHEMA.{object_type}
                where {obj_schema}_catalog = '{db_name}'
                  AND {obj_schema}_schema = '{schema}' """
                if object_type == 'tables':
                    sql += """ and table_type ='BASE TABLE' """
                sql += """ order by 1 """

                cursor.execute( sql )
                results = cursor.fetchall()
                print(f"\t>>> Found {len(results)} {object_type}")
                if db_name not in database_structure:
                    database_structure[db_name] = {}
                if schema not in database_structure[db_name]:
                    database_structure[db_name][schema] = {}
                if object_type not in database_structure[db_name][schema]:
                    database_structure[db_name][schema][object_type] = []
                database_structure[db_name][schema][object_type].extend( [result[0] for result in results] )
            # print( database_structure ); break

        # Now that we have the database structure, write it out to a file and create a directory structure to mimic in the output_dir
                # e.g. output_dir/database_name/schema_name/

    # Create the directory structure
    for db_name in database_structure.keys():
        db_dir = root/output_dir/Path(db_name)
        print(f">>> Creating directories under {db_dir}")
        db_dir.mkdir(parents=True, exist_ok=True) 
        for schema in database_structure[db_name].keys():
            schema_dir = db_dir/Path(schema)
            schema_dir.mkdir(parents=True, exist_ok=True)

            try:
                print(f'>>> Creating SCHEMA DDL under {schema_dir}')
                # Generate the SCHEMA get_DDL as a file
                sql = f"""select get_ddl('schema', '{db_name}.{schema}');"""
                cursor.execute( sql )
                ddl = cursor.fetchall()[0][0]

                schema_file = schema_dir/Path(f"SCHEMA-{schema}.sql")
                print(f">>> Writing {schema_file}")
                with schema_file.open('w') as f:
                    f.write( ddl )
            except:
                print(f"Failed to retrieve SCHEMA DDL: {schema}")
                pass


            # Most likely will remove the below...
            for object_type in database_structure[db_name][schema].keys():
                object_dir = schema_dir/Path(object_type)
                object_dir.mkdir(parents=True, exist_ok=True)

                # Write out the files
                # e.g. output_dir/database_name/schema_name/object_type/object_name.sql
                for object_name in database_structure[db_name][schema][object_type]:
                    try:
                        if object_type in( 'functions', 'procedures') :
                            print( f">>> Attempting to retrieve {object_type.rstrip('s').upper()} DDL:  {db_name}, {schema}, {object_name} ")
                            ddl = get_ftype_ddl( cursor, db_name, schema, object_type.rstrip('s'), object_name )
                        else:
                            sql = f"""select get_ddl('{object_type.rstrip("s")}', '{db_name}.{schema}.{object_name}');"""
                            cursor.execute( sql )
                            ddl = cursor.fetchall()[0][0]

                        object_file = object_dir/Path(f"{object_name}.sql")
                        print(f">>> Writing {object_file}")
                        with object_file.open('w') as f:
                            f.write( ddl )
                    except:
                        print(f"Failed to retrieve {object_type}: {object_name}")
                        pass


if __name__ == "__main__":
    main()

# python extract_db_objects.py -d EDP_BRONZE_DEV -s ABHINAV_KODANDA    
# python extract_db_objects.py --src dev/fbronze/PLAYGROUND -s ROBERT_METZKER    
