from datetime import datetime

dts = ['01/02/2023','Feb 12, 2023','Smith, John','M423 TECHNOLOGIES, LLC']
fmt = ['%m/%d/%Y','%b %d, %Y']

for dt in dts:
    for f in fmt:
        try:
            new = datetime.strptime(dt, f).strftime('%Y-%m-%d')
            new = f'{dt} + {f} = {new}'
            break
        except:
            new = dt

    print(f'OUTPUT: {new}')


