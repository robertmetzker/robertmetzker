a = '$10.00'

a = a.replace('$', '')
if a.isnumeric:
    print( a )