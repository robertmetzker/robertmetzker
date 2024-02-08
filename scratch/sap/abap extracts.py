import re

# Config
ABAP_FILE = 'open orders to ZSALES 3_5.abap'
SQL_OUTPUT = 'converted.sql'

# Mapping ABAP structures to tables
MAPPING = {
    'comm_structure': 'comm_table',
    'g_s_icvw': 'icvw_table'
}

# Open ABAP file and read contents
with open(ABAP_FILE, 'r') as f:
    abap_code = f.read()
    
# Find all ABAP data structures    
structures = re.findall(r'(BEGIN OF (\w+)\.)(.*?)END OF \2', abap_code, re.DOTALL)

def get_sql(code):

    statements = []
    
    # Process code to generate SQL statements
    for loop in re.findall(r'LOOP AT (\w+) INTO (\w+)', code):
        table = MAPPING.get(loop[0], loop[0]) 
        record = loop[1]
        
        columns = re.findall(r'\s*(\w+) TYPE.*', 
                        next(s for s in structures if s[1] == record)[2])
                
        statements.append(f"SELECT * FROM {table}")
                          
        for field in re.findall(rf'{record}-\>(\w+) = (\w+)', code):
            statements.append(f"""UPDATE {table} SET {field[0]} = {field[1]}
                                   WHERE <key> = <value>""")
                                   
    for append in re.findall(r'APPEND (\w+) TO (\w+)', code):
        record = append[0]
        table = MAPPING.get(append[1], append[1])
        
        columns = re.findall(r'\s*(\w+) TYPE.*',  
                        next(s for s in structures if s[1] == record)[2])
                
        statements.append(f"""
    INSERT INTO {table} ({', '.join(columns)})  
    VALUES ({record})""")
                           
    return statements
    
    
sql_statements = get_sql(abap_code) 

# Write SQL statements to output file
with open(SQL_OUTPUT, 'w') as f:
    for stmt in sql_statements:
        f.write(stmt + ';\n')