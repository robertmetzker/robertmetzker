from pathlib import Path

root='C:\Temp\dev\vertica_etl\rpd1\dw_report\data'

root=Path('C:/Temp/dev')

for afile in root.rglob('*.gz'):
    print(afile)