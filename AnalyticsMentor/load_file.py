import psycopg2, argparse, regex
from datetime import datetime
from config import config

# Set up some defaults for the CLI interface.  Either Use the fullword or shorthand
# Example: python load_file.py --fname ../data/2019-04-01.csv --drop --run --load
def get_args():
    parser = argparse.ArgumentParser(description='Connect to a Postgres DB')
    parser.add_argument('--find', help='Directory path to process CSV files', required=True)
    parser.add_argument('-f', '--fname', help='FilePath/Filename to build a table for', required=True)
    parser.add_argument('-d', '--drop', default=False, action='store_true', help='-d to drop the table if it exists')
    parser.add_argument('-r', '--run', default=False, action='store_true', help='-r to execute the SQL built')
    parser.add_argument('-v', '--validate', default=False, action='store_true', help='-v to execute SQL validation counts')
    parser.add_argument('-l', '--load', default=False, action='store_true', help='-l to load the specified file into a table with the same name')
    args = parser.parse_args()

    return args


def get_files(path):
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
            list_str = list_str.replace(' ', '_')
            header_list[i] = ''.join(e for e in list_str if e.isalnum())

        header = ','.join(header_list)
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


# Run the sql against the database (makes a new connection each time to force a commit)
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
        comma_separated_list.append( chunk )

    return comma_separated_list


def load_data( p_db_name, p_user, p_pass, table_name, fname, coltypes ):
    conn = connect_postgres(p_db_name, p_user, p_pass)
    cur = conn.cursor()

    print(f'\n\n-- LOADING DATA from {fname} into {table_name}')

    # sql = f"COPY {table_name} FROM {fname} DELIMITER ', ' CSV HEADER;"

    # Use coltypes to determine if the column needs to be wrapped in quotes
    # only BIGINT and DECIMAL should not be wrapped in quotes
    # Make a true/false list of whether to wrap the column in quotes
    wrap = []
    for col in coltypes:
        if coltypes[col] == 'BIGINT' or coltypes[col] == 'DECIMAL':
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
            # For each item in line_items, determine if it needs to be wrapped in quotes
            # if it does, then wrap it in quotes
            for i in range(len(line_items)):
                item = line_items[i]
                item = item.replace('"', '')
                item = item.replace("'", "''")
                item = item.strip().replace("\n","")
                item = item.replace("Not Available", "NULL")
                # print(f"line_items[{i}]: {item}")

                if item.upper() == 'NULL' and not wrap[i]:
                    line_items[i] = item
                elif item.upper() == 'NULL':
                    line_items[i] = 'NULL'

                # Missing Numeric values are being converted to 0
                elif item.startswith('$'):
                    item = item.replace("$", "")
                    # if item.isnumeric():
                    line_items[i] = item
                elif not wrap[i] and not line_items[i]:
                    line_items[i] = '0'
                elif wrap[i]:
                    line_items[i] = f"'{item}'"
            new_line = ','.join(line_items) + ');'
            sql += new_line
            all_sql.append(sql)

    # print(f'ALL SQL:{all_sql}')

    print(f'-- TRUNCATING existing table...\n')
    cur.execute(f'TRUNCATE TABLE {table_name};')
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

    # hw_file = 'HW/drinks.csv'
    hw_file = args.fname

    header, sample = get_file_header(hw_file)
    coltypes = get_datatypes(header, sample)
    

    # determine the table name based on the name of the hw_file
    # get the last portion of the path and split at the period
    # then remove any spaces and replace with underscores
    table_name = hw_file.split('/')[-1].split('.')[0].replace(' ', '_')

    if args.drop:
        drop_sql = f'\t DROP TABLE IF EXISTS {table_name};'
        print(f'-- DROPPING {table_name}:\n{drop_sql}')
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

    if args.load:
        load_data(p_db_name, p_user, p_pass, table_name, hw_file, coltypes )

    if args.validate:
        print(f'-- VALIDATING table counts for {table_name}...')
        validate_sql = f'\t SELECT count(1) FROM {table_name};'
        execute_sql(p_db_name, p_user, p_pass, validate_sql)


if __name__ == '__main__':
    main()

# python load_file.py -f HW/drinks.csv  -d -r -v -l
# python load_file.py -f HW/foods.csv  -d -r -v -l
# python load_file.py -f HW/food_inventories.csv -d -r -v -l

# python load_file.py -f adv/products.csv -d -r -v -l
# python load_file.py -f adv/stores.csv -d -r -v -l
    # python load_file.py -f adv/customers.csv -d -r -v -l
    # python load_file.py -f adv/transactions.csv -d -r -v -l
    # python load_file.py -f adv/transaction_items.csv -d -r -v -l
# python load_file.py -f adv/daily_sales_summary.csv -d -r -v -l

# python load_file.py -f Patient_Survey/data_tables/measures.csv  -d -r -v -l
# python load_file.py -f Patient_Survey/data_tables/national_results.csv  -d -r -v -l
# python load_file.py -f Patient_Survey/data_tables/questions.csv  -d -r -v -l
# python load_file.py -f Patient_Survey/data_tables/reports.csv  -d -r -v -l
# python load_file.py -f Patient_Survey/data_tables/responses.csv  -d -r -v -l
# python load_file.py -f Patient_Survey/data_tables/state_results.csv  -d -r -v -l
# python load_file.py -f Patient_Survey/data_tables/states.csv  -d -r -v -l
