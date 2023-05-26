import pyodbc

#DEP_CT_SQL, 50733

print(pyodbc.drivers())
#['SQL Server', 'Microsoft Access Driver (*.mdb, *.accdb)', 'Microsoft Excel Driver (*.xls, *.xlsx, *.xlsm, *.xlsb)', 'Microsoft Access Text Driver (*.txt, *.csv)', 'SnowflakeDSIIDriver']
driver='{SQL Server}'
server='DEP_CT_SQL' 
database='DEP'
conn_str=f"Driver={driver};Server={server},50733;DATABASE={database};Trusted_Connection=yes;"
print(conn_str)

conn =  pyodbc.connect(conn_str)
cursor = conn.cursor()

sql='SELECT * FROM INFORMATION_SCHEMA.TABLES'

query=cursor.execute(sql)
columns = [column[0] for column in cursor.description]

for row in query.fetchall():
    row_dict=dict(zip(columns, row))
    print(row_dict)
    query2=f'select * from {row_dict["TABLE_CATALOG"]}.{row_dict["TABLE_SCHEMA"]}.[{row_dict["TABLE_NAME"]}]'
    cursor.execute(query2)
    columns2 = [column[0] for column in cursor.description]
    for row2 in cursor.fetchmany(2):
        row_dict2=dict(zip(columns2, row2))
        print(f'\t{row_dict2}')
        break
    print('-------------------')
