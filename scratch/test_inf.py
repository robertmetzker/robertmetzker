import sys, os, datetime
from pathlib import Path
prog_path = Path(os.path.abspath(__file__))
root = prog_path.parent.parent
lclsetup = root/f"lclsetup"
sys.path.append(root)
sys.path.append(lclsetup)

# print( "Using paths:\n", root,'\n', lclsetup, '\n','-'*80 )
from lclsetup import dbsetup, inf 


def main():
    """
    # --prev dev/bronze/INC_DEV_SOURCE --curr prd/bronze/EDP_BRONZE_PROD  --schema SHOPIFY_FIBERON --init --debug 
    """
    env = 'dev'
    db = 'funct'
    schema = 'ROLLBACKS'
    config = dbsetup.config[0]

    # Create a pkg to send to inf.get_dbcon to pick the env,db connection
    pkg = { 'config':config, 'env':env, 'db':db, 'schema':schema} 

    # Test prints to validate import/contents of config
    # print( f"dbsetup retrieved: {str(dbsetup)}")
    # print( f"{str( config['env'][env][db] )}")

    # Test establishing a connection
    print("\n","="*40,"\n Establshing a Connection...\n","="*40)

    # Run a sample query (getting currnet datetime)
    # sql = 'select current_timestamp()'
    # all_args = (pkg, sql)
    # result = inf.run_sql( all_args )

    # # Output the results
    # print(f"SQL Results:\n{sql}\n\n{result}")

    get_sql = inf.read_sql( 'today.sql')
    print( f"Read from file: {get_sql}" )
    results = ''
    
    now = datetime.datetime.now()
    print("-" * 40, f"Testing RUN_SQL @ {now.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 40)
    # print( sql20 )
    for sql in get_sql: 
        all_args = (pkg, sql )
        # results = inf.run_sql( all_args )
        print( results )

    all_args = (pkg, get_sql )
    results = inf.run_sql_async( all_args )
    print(f'testing ASYNC:{ results }')

    all_args = ('GET DATES', pkg, get_sql )
    results = inf.run_sql_parallel( all_args )
    print(f'testing PARALLEL:{ results }')

    """
    # Generate 5 select queries and run in async mode
    now = datetime.datetime.now()
    print("-" * 40, f"Testing ASync @ {now.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 40)
    sql5 = [f"select current_timestamp() as time_{i}" for i in range(5)]
    # print( sql20 )
    all_args = (pkg, sql5)
    async_results = inf.run_sql_async( all_args )
    now = datetime.datetime.now()
    print("-" * 40, f"Finished @ {now.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 40)
    # print(f"\n{async_results}\n")

    # Generate 20 select queries and run in parallel
    now = datetime.datetime.now()
    print("-" * 40, f"Testing Parallel @ {now.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 40)
    sql20 = [f"select current_timestamp() as time_{i}" for i in range(21)]
    all_args = ('CONCURRENT DATE QUERIES', pkg, sql20)
    parallel_results = inf.run_sql_parallel( all_args )
    now = datetime.datetime.now()
    print("-" * 40, f"Finished @ {now.strftime('%Y-%m-%d %H:%M:%S')}", "=" * 40)
    # print(f"\n{parallel_results}\n")
    """



    print("DONE")
    # print(f"In parallel: {new_results}")

if __name__ == '__main__':
    main()
