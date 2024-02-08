import os
import re

def convert_ddl_to_select(input_directory):
    # Regex to match the CREATE TABLE line and extract the table name
    create_table_regex = re.compile(r'CREATE TABLE "(.+?)"\."(.*?)"')
    # Regex to match column names
    column_name_regex = re.compile(r'\s*"(\w+)"')

    for filename in os.listdir(input_directory):
        if filename.endswith('.sql'):
            input_path = os.path.join(input_directory, filename)
            with open(input_path, 'r') as file:
                content = file.read()

                # Find the table name
                create_table_match = create_table_regex.search(content)
                if create_table_match:
                    schema_name, table_name = create_table_match.groups()
                    table_full_name = f"{schema_name}.{table_name}".replace('"', '')

                    # Find all column names
                    columns = column_name_regex.findall(content)

                    # Construct SELECT statement
                    columns_select = ',\n\t'.join(columns)
                    select_statement = f"""SELECT {columns_select}\nFROM {table_full_name}\n"""

                    # Output file path
                    # Only replace the first period with an underscore to avoid replacing the schema name and leave the .sql extension
                    output_filename = f"""extract-{schema_name}_{table_name}.sql""".replace('"', '')
                    # Output to a extract_sql subfolder, creating it if it does not exist
                    output_directory = os.path.join(input_directory, 'extract_sql')
                    if not os.path.exists(output_directory):
                        os.makedirs(output_directory)

                    output_path = os.path.join(output_directory, output_filename)
                    # Create the output_path if it does not already exist
                    # Save the SELECT statement to a new file
                    with open(output_path, 'w') as output_file:
                        output_file.write(select_statement)

                    print(f"Processed {filename} -> {output_filename}")

if __name__ == "__main__":
    input_directory = './emtek/ddl'  # Update this path to your specific folder
    convert_ddl_to_select(input_directory)
