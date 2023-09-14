import psycopg2, argparse, glob
from datetime import datetime
from config import config

def get_args():
    parser = argparse.ArgumentParser(description='Connect to a Postgres DB')
    parser.add_argument('-f', '--fname', help='FilePath/Filename to build a table for')
    parser.add_argument('-d', '--drop', default=False, action='store_true', help='-d to drop the table if it exists')
    parser.add_argument('-r', '--run', default=False, action='store_true', help='-r to execute the SQL built')
    parser.add_argument('-v', '--validate', default=False, action='store_true', help='-v to execute SQL validation counts')
    parser.add_argument('-l', '--load', default=False, action='store_true', help='-l to load the specified file into a table with the same name')
    parser.add_argument('-find', '--find', help='Path to directory for searching .csv files')
    args = parser.parse_args()

    return args


def get_files(path):
    # use glob to find all csv files in path
    csv_files = []
    csv_files = glob.glob(f"{path}/*.csv")

    return csv_files


def get_file_header( fname ):
# Read a file and read the first line as a header
    with open(fname, 'r') as f:
        header = f.readline()
        # read the next line as sample data
        sample = f.readline()

    return header, sample


def get_datatypes( header, sample ):
    # Determine the data types of each column
    # Assume all columns are varchar(255) unless they are integers or floats
    # If a column is an integer, then BIGINT is used
    # If a column is a float, then DECINAL is used
    # If a column is a date, then DATE is used
    possible_data_types = ['BIGINT', 'DECIMAL', 'DATE']        

    # Split the header and sample into lists
    header_list = header.split(',')
    sample_list = sample.split(',')

    # Create a list to hold the data types
    data_types = []
    date_formats = ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f']

    # Loop through the sample list and determine the data types by testing against possible_data_types
    for i in range(len(sample_list) ):
        # print(sample_list[i])
        # print(type(sample_list[i]))
        if sample_list[i].strip().isnumeric():
            data_types.append('BIGINT')
        # elif sample_list[i].isdecimal():
        elif '.' in sample_list[i] and sample_list[i].replace('.','').strip().isnumeric():
            data_types.append('DECIMAL')
        # compare data formats to determine if the column is a date
        elif '-' in sample_list[i]:
            for df in date_formats:
                try:
                    datetime.strptime(sample_list[i], df)
                    data_types.append('DATE')
                    break
                except ValueError:
                    data_types.append('VARCHAR(255)')
                    break
        else: # sample_list[i].strip().isalpha():
            data_types.append('VARCHAR(255)')
            # TODO: Add logic to determine if the column is a date
        # print(f'{header_list[i].strip()} is {data_types[i]}: {sample_list[i]}')
        
    # Display the column name and determined data types
    # combine into a dictionary
    cols = {}
    for i in range(len(header_list)):
        cols[header_list[i].strip()] = data_types[i]
    
    return cols


def execute_sql(p_db_name, p_user, p_pass, p_query ):
    conn = None
    try:

        # connect to the PostgreSQL server
        print('\n-- Attempting to connect...')
        conn = connect_postgres(p_db_name, p_user, p_pass)
        # conn = psycopg2.connect("dbname=" + p_db_name + " user=" + p_user + " password=" + p_pass)

        # create a cursor
        cur = conn.cursor()
        
        # execute a statement
        print( f'-- Executing SQL...\n/* \n{p_query}\n*/')
        if 'select' in p_query.lower():
            cur.execute(p_query)
            print( cur.fetchone() )
        else:
            cur.execute(p_query)
            print('\n-- SQL executed.')
            print( f'\t-- {cur.statusmessage}' )

            print(f'\n\t -- COMMITTING changes...')
            conn.commit()

        # close the communication with the PostgreSQL
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('\t -- DB Conn closed.')


def main():
    args = get_args()
    p_db_name, p_user, p_pass = config

    if args.find:
        csv_files = get_files(args.find)
        print(f'Found {len(csv_files)} files:  {csv_files}')

        for file in csv_files:
            print(f'Processing file: {file}')
            # Now you can call your function on each file
            # You may need to adjust your functions to take in a filename
            # You might also need to adjust your functions to dynamically generate the table name
            header, sample = get_file_header(file)
            coltypes = get_datatypes(header, sample)
            table_name = file.split('/')[-1].split('.')[0].replace(' ', '_')
            if args.drop:
                drop_sql = f'\t DROP TABLE IF EXISTS {table_name};'
                print(f'-- DROPPING {table_name}:\n{drop_sql}')
                execute_sql(p_db_name, p_user, p_pass, drop_sql)
            sql = build_create_table_sql(table_name, coltypes)
            if args.run:
                execute_sql(p_db_name, p_user, p_pass, sql)
            if args.load:
                load_data(p_db_name, p_user, p_pass, table_name, file)
            if args.validate:
                print(f'-- VALIDATING table counts for {table_name}...')
                validate_sql = f'\t SELECT count(1) FROM {table_name};'
                execute_sql(p_db_name, p_user, p_pass, validate_sql)
                
    else:
        hw_file = args.fname if args.fname else None
        if hw_file:
            header, sample = get_file_header(hw_file)
            coltypes = get_datatypes(header, sample)
            table_name = hw_file.split('/')[-1].split('.')[0].replace(' ', '_')
            if args.drop:
                drop_sql = f'\t DROP TABLE IF EXISTS {table_name};'
                print(f'-- DROPPING {table_name}:\n{drop_sql}')
                execute_sql(p_db_name, p_user, p_pass, drop_sql)
            sql = build_create_table_sql(table_name, coltypes)
            if args.run:
                execute_sql(p_db_name, p_user, p_pass, sql)
            if args.load:
                load_data(p_db_name, p_user, p_pass, table_name, hw_file)
            if args.validate:
                print(f'-- VALIDATING table counts for {table_name}...')
                validate_sql = f'\t SELECT count(1) FROM {table_name};'
                execute_sql(p_db_name, p_user, p_pass, validate_sql)

if __name__ == '__main__':
    main()

# python load_all.py -find Patient_Survey/data_tables/*.csv  -d -r -v -l
