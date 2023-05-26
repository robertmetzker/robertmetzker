import csv
from pathlib import Path

fw_file='I:/Data_Lake/IT/ETL/a85275/EL/dev/mainframe/cam/logs/2021_07_10/fw_linecounts.csv'
ld_file='I:/Data_Lake/IT/ETL/a85275/EL/dev/vertica_me2/A85275/logs/2021_07_10/report_2021_07_15T162558_run_load.py.csv'

fwd={}
ldd={}

# %%
def compare( fwd, ldd ):
    excl=('ld_table','data_file')
    tablelist = fwd.keys() | ldd.keys()

    # Excluding tables from comparison
    for table in list(filter(lambda table : table not in excl , tablelist)):

        try:
            diff = int(float(ldd.get(table,0))) - int(float(fwd.get(table,0)))
        except:
            diff= -1
        if diff==0: 
            stat='OK' 
        else:
            stat='<= POTENTIAL ERROR'
        print(f'{str(diff):>8s} rows different when loading {str(ldd.get(table,0)):>8s} rows for {table:<25s}{stat}.')


# data_file, bytes, fixed_width, total_lines
with open( fw_file, newline='' ) as fw:
    fw_reader=csv.reader(fw, delimiter=',')
    fwd = {rows[0]:rows[3] for rows in fw_reader }

print(f'FWD: =================== \n{fwd}')


# ld_count, ld_error, ld_fname, ld_parent_end, ld_parent_runtime, ld_parent_start, ld_rate, ld_runtime, ld_status, ld_table, ld_warning
with open( ld_file, newline='' ) as ld:
    ld_reader=csv.reader(ld, delimiter=',')
    ldd = {rows[9]:rows[0] for rows in ld_reader}

print(f'LDD: =================== \n{ldd}')

# %%    
for (key,value) in set(fwd.items()) & set(ldd.items()):
    print('%s : %s  matched in both files...' %(key,value) )

compare(fwd,ldd)
