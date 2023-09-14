import glob, psycopg2, argparse, regex as re
from datetime import datetime
from config import config

# Set up some defaults for the CLI interface.  Either Use the fullword or shorthand
# Example: python load_all_files.py --fname ../data/2019-04-01.csv --drop --run --load
def get_args():
    parser = argparse.ArgumentParser(description='Connect to a Postgres DB')
    parser.add_argument('--find', help='Directory path to process CSV files', required=False)
    parser.add_argument('--file', help='FilePath/Filename to build a table for', required=False)
    parser.add_argument('--copy', action='store_true', help='Use COPY instead of INSERT to load data') 
    parser.add_argument('-d', '--drop', default=False, action='store_true', help='-d to drop the table if it exists')
    parser.add_argument('-r', '--run', default=False, action='store_true', help='-r to execute the SQL built')
    parser.add_argument('-v', '--validate', default=False, action='store_true', help='-v to execute SQL validation counts')
    parser.add_argument('-l', '--load', default=False, action='store_true', help='-l to load the specified file into a table with the same name')
    args = parser.parse_args()

    return args


def get_csv_files(path):
    # use glob to find all csv files in path
    return glob.glob(f"{path}/*.csv")


# Connect to postgres using the config.py file
# It should contain a single line to unpack:
# config = ['database_name', 'username', 'password']
def connect_postgres(p_db_name, p_user, p_pass):
    conn = psycopg2.connect("dbname=" + p_db_name + " user=" + p_user + " password=" + p_pass)
    return conn


def get_file_header( fname ):
# Read a file and read the first line as a header
    with open(fname, 'r') as f:
        header = f.readline()
        header_list = header.split(',')

        for i in range(len(header_list)):
            list_str = header_list[i]
            clean_str = list_str.replace(' ','_').lower()
            # remove any non-alphanumeric characters
            clean_str = re.sub(r'\W+', '', clean_str)
            # remove any leading/trailing underscores
            clean_str = re.sub(r'^_', '', clean_str)
            clean_str = re.sub(r'_$', '', clean_str)
            header_list[i]  = clean_str

            # list_str = list_str.replace(' ', '_')
            # header_list[i] = ''.join(e for e in list_str if e.isalnum())

        header = ','.join(header_list)
        # read the next line as sample data
        sample = f.readline()

    return header, sample


def get_datatypes( header, sample ):
    # Determine the data types of each column
    possible_data_types = ['NUMERIC', 'TEXT', 'DATE', 'TIMESTAMP']        

    # Split the header and sample into lists
    header_list = header.split(',')
    sample_list = sample.split(',')

    # Create a list to hold the data types
    data_types = []
    date_formats = ['%Y-%m-%d', '%m-%d-%Y', '%m/%d/%Y', '%Y/%m/%d']
    ts_formats = [ '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f' ]
    # Loop through the sample list and determine the data types by testing against possible_data_types
    for i in range(len(sample_list) ):
        col = sample_list[i]
        col = col.strip('\n')
        # print(type( col ))
        if '-' in col:
            if ':' in col:
                for df in ts_formats:
                    try:
                        col.strip('\n')
                        datetime.strptime(col, df)
                        data_types.append('TIMESTAMP')
                        break
                    except ValueError:
                        data_types.append('TEXT')
                        break
            else:
                for df in date_formats:
                    try:
                        col.strip('\n')
                        datetime.strptime(col, df)
                        data_types.append('DATE')
                        break
                    except ValueError:
                        data_types.append('TEXT')
                        break
        elif re.match(r'^\d+$', col):
            # Only contains digits, treat as a number
            data_types.append('NUMERIC')
        else:
            data_types.append('TEXT')

    # Display the column name and determined data types
    # combine into a dictionary
    cols = {}
    for i in range(len(header_list)):
        cols[header_list[i].strip()] = data_types[i]
    
    return cols


# Run the sql against the database (makes a new connection each time to force a commit)
def execute_sql(p_db_name, p_user, p_pass, p_query ):
    conn = None
    result = []
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
            result =  cur.fetchone()
            print( result )
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

    return result

def tokenize( string, separator = ',', quote = '"' ):
    """
    Split a comma separated string into a List of strings.

    Separator characters inside the quotes are ignored.

    :param string: A string to be split into chunks
    :param separator: A separator character
    :param quote: A character to define beginning and end of the quoted string
    :return: A list of strings, one element for every chunk
    """
    comma_separated_list = []

    chunk = ''
    in_quotes = False

    for character in string:
        if character == separator and not in_quotes:
            comma_separated_list.append(chunk)
            chunk = ''

        else:
            chunk += character
            if character == quote:
                in_quotes = False if in_quotes else True

    if chunk != '\n':
        chunk = chunk.strip('\n')
        comma_separated_list.append( chunk )

    return comma_separated_list


def copy_into_table( p_db_name, p_user, p_pass, table_name, fname ):
    conn = connect_postgres(p_db_name, p_user, p_pass)
    cur = conn.cursor()
    print(f'\n\n-- COPYING DATA from {fname} into {table_name} using \copy')

    with open(fname, 'r') as f:
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)
        conn.commit()


def load_data( p_db_name, p_user, p_pass, table_name, fname, coltypes ):
    conn = connect_postgres(p_db_name, p_user, p_pass)
    cur = conn.cursor()

    print(f'\n\n-- LOADING DATA from {fname} into {table_name}')

    # Use coltypes to determine if the column needs to be wrapped in quotes
    wrap = []
    for col,dtype in coltypes.items():
        if dtype == 'NUMERIC':
            wrap.append(False) 
        else:
            wrap.append(True)

    # print(wrap)

    # read the fname and convert each line into an insert statement.
    # skip the first linea and use the header to build the insert statement
    # combine them into a list and then execute them all at once
    all_sql = []
    with open(fname, 'r') as f:
        lines = f.readlines()
        lines = lines[1:]
        # print(lines)
        for line in lines:
            sql = f"INSERT INTO {table_name} VALUES ("
            # split on comma unless it is wrapped in quotes
            line_items = tokenize(line)
            # print(line_items)

            # line_items = line.split(',')
            for i in range(len(line_items)):
                item = line_items[i]
                item = item.replace('"', '')
                item = item.replace("'", "''")
                item = item.strip().replace("\n","")
                item = item.replace("Not Available", "NULL")

                # Missing Numeric values are being converted to 0
                if item.startswith('$'):
                    item = item.replace("$", "")
                    # if item.isnumeric():
                else:
                    line_items[i] = item

                if not wrap[i]:
                    if item == '':
                        line_items[i] = '0'
                    else:
                        line_items[i] = item
                else:
                    line_items[i] = f"'{item}'" if item != '' else "NULL"

            new_line = ','.join(line_items) + ');'
            sql += new_line
            all_sql.append(sql)

    # print(f'ALL SQL:{all_sql}')

    print(f'-- TRUNCATING existing table...\n')
    cur.execute(f'TRUNCATE TABLE {table_name} CASCADE;')
    conn.commit()

    print(f'-- SQL INSERTS executing... \n')
    for sql in all_sql:
        print(sql)
        cur.execute(sql)
        conn.commit()
    # print( cur.statusmessage )
    print(f'-- SQL INSERTS executed: {len(all_sql)} statements... \n')

    print(f'\n-- COMMITTING changes...')
    conn.commit()


def main():
    args = get_args()
    p_db_name, p_user, p_pass = config
    summary = {}

    if args.find:
        csv_files = get_csv_files(args.find)
    else:
        csv_files = [args.fname]

    for csv_file in csv_files:
        header, sample = get_file_header(csv_file)
        coltypes = get_datatypes(header, sample)

        table_name = csv_file.split('/')[-1].split('.')[0].replace(' ', '_')
        # table_name = get_table_name(csv_file) 

        # determine the table name based on the name of the hw_file
        # get the last portion of the path and split at the period
        # then remove any spaces and replace with underscores
        # table_name = hw_file.split('/')[-1].split('.')[0].replace(' ', '_')

        if args.drop:
            drop_sql = f'\t DROP TABLE IF EXISTS {table_name};'
            print(f'-- DROPPING {table_name}:\n{drop_sql}\n\n')
            execute_sql(p_db_name, p_user, p_pass, drop_sql)

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"

        # Concatenate col, dtype into a string and join all with commas
        cols = []
        for col, dtype in coltypes.items():
            cols.append( f"\t{col}\t{dtype}\n") 

        sql += ', '.join(cols)
        sql += ') ;'

        if not args.run:
            print(f'/* GENERATED:\n{sql}\n*/')
            # sql = 'select * from customers where customer_id = 1'
            # execute_sql(p_db_name, p_user, p_pass, sql)

        elif args.run:
            execute_sql(p_db_name, p_user, p_pass, sql)

        if args.copy:
            copy_into_table(p_db_name, p_user, p_pass, table_name, csv_file)
        elif args.load:
            load_data(p_db_name, p_user, p_pass, table_name, csv_file, coltypes )

        if args.validate:
            print(f'-- VALIDATING table counts for {table_name}...')
            validate_sql = f'\t SELECT count(1) FROM {table_name};'
            results = execute_sql(p_db_name, p_user, p_pass, validate_sql)
            print(f'\t\t{table_name}: {results}')
            if results and results[0]:
                # print(f'\t\t{table_name}: {results[0]}')
                summary[table_name] = results[0]
            print(f'='*120,'\n\n')

    if summary:
        print(f'\n\n-- SUMMARY of table counts:\n')
        for table, count in summary.items():
            print(f'\t{table:<35}: {count:>15}')
        print(f'\n\n')


if __name__ == '__main__':
    main()

# python load_all_files.py --find HW -d -r -v -l

# python load_all_files.py --find adv --copy -d -r -v

# python load_all_files.py --find Patient_Survey/data_tables --copy -d -r -v

# python load_all_files.py --find samples --copy -d -r -v
