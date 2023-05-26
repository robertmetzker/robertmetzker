# default imports
import os
from pathlib import Path

# specific imports
import shutil, datetime, math, openpyxl
from sheet2dict import Worksheet
from openpyxl import Workbook
'''
    NOTE: Currently uses shutil.copy2  >>  use shutil.move   to actually move
'''

def countlines( file_tomove ):
    # Powershell equivalent:  Get-Content file_tomove | Measure-Object -Line
    with open( file_tomove, 'r') as fp:
        for count, line in enumerate(fp):
            pass
    
    return count + 1


def copydirectory( from_loc, to_loc, extn ):
    src_path = Path( from_loc)
    tgt_path = Path( to_loc )
    os.makedirs( Path( to_loc) , exist_ok = True)
    file_list = []
    
    created =  datetime.datetime.now()
    found_list = os.listdir( from_loc )
    if extn == '*': 
        file_list = found_list
    else:
        for f in found_list:
            if f.endswith(extn): file_list.append(f)
            
    lines, total_lines = 0, 0

    print( f'\n====\n Moving {len(file_list)} files in {from_loc}\n\t\tto {to_loc}:')
    for file in file_list:
        sourceFile = os.path.join( src_path, file)
        cdate = math.trunc(  os.path.getmtime( sourceFile ) )
        file_created = datetime.datetime.fromtimestamp( cdate )

        print( f"\t=> Moving {sourceFile:<20}\n\t\t to {tgt_path}"  )
        shutil.copy2(sourceFile, tgt_path )
        lines = countlines( sourceFile )
        total_lines += lines
        if file_created < created:
            created = file_created

    return created, total_lines


def combinedirectory( from_loc, to_loc, combine2, extn ):
    src_path = Path( from_loc)
    tgt_path = Path( to_loc, combine2 )
    os.makedirs( Path(to_loc), exist_ok = True)
    file_list = []
    
    created =  datetime.datetime.now()
    found_list = os.listdir( from_loc )
    if extn == '*': 
        file_list = found_list
    else:
        for f in found_list:
            if f.endswith(extn): file_list.append(f)

    num_lines, total_lines = 0, 0

    print( f'\n====\n Combining {len(file_list)} files:\n[{file_list}]\n\t in {from_loc}\n\t\tto {to_loc}:')
    for file in file_list:
        if file == 'None': continue
        sourceFile = os.path.join( src_path, file)
        cdate = math.trunc(  os.path.getmtime( sourceFile ) )
        file_created = datetime.datetime.fromtimestamp( cdate )

        if tgt_path.is_file():
            cdate = math.trunc(  os.path.getmtime( tgt_path ) )
            created = datetime.datetime.fromtimestamp( cdate )
            
        print( f"Combining {sourceFile:<20} into {combine2}"  )
        num_lines, file_created = combinefile( from_loc, to_loc, sourceFile, combine2, created )
        total_lines += num_lines
        if file_created < created:
            created = file_created

    return created, total_lines


def copyfile( from_loc, to_loc, file_tomove, created ):
    src_path = Path( from_loc, file_tomove)
    tgt_path = Path( to_loc, file_tomove)
    num_lines, cdate = 0, 0
    file_created = None
    
    # Write out the creation timestamp for each file
    os.makedirs( Path(to_loc), exist_ok = True)
    print(f'\n > Move {file_tomove:<20} from {from_loc}')

    try:
        # truncate the milliseconds from the mtime since xls does not store them at the same precision for comparison
        # file_created = datetime.datetime.fromtimestamp( os.path.getmtime( src_path ))
        # e.g. 1645649851.817844
        cdate = math.trunc(  os.path.getmtime( src_path ) )
        file_created = datetime.datetime.fromtimestamp( cdate )
        file_created_str = str(file_created)[0:19]

        if not created == file_created_str:
            num_lines = countlines( src_path )
            shutil.copy2( src_path, tgt_path ) 
            print(f'\tsuccessfully copied to {to_loc}... ')
        else:
            print(f' ### NOTE: >>> {file_tomove:<20}  was skipped as already copied...')
    except FileNotFoundError:
        print(f' ### ERROR >>> {file_tomove:<20}  was not found in {from_loc}')
    except:
        print(f'\nFAILED to copy to {to_loc}... ')

    return num_lines, file_created


def combinefile( from_loc, to_loc, file_tomove, combine2, created ):
    src_path = Path( from_loc, file_tomove)
    tgt_path = Path( to_loc, combine2)
    num_lines, cdate = 0, 0
    file_created = None

    # Write out the creation timestamp for each file
    os.makedirs( Path(to_loc), exist_ok = True)
    print(f'\t=> Appending {file_tomove:<20}')

    try:
        # truncate the milliseconds from the mtime since xls does not store them at the same precision for comparison
        # file_created = datetime.datetime.fromtimestamp( os.path.getmtime( src_path ))
        # e.g. 1645649851.817844
        cdate = math.trunc(  os.path.getmtime( src_path ) )
        file_created = datetime.datetime.fromtimestamp( cdate )
        file_created_str = str(file_created)[0:19]
        if created == file_created_str:
            num_lines = countlines( src_path )
            print(f' ### NOTE: >>> {file_tomove:<20}  was skipped as already appended...')
            return num_lines, file_created
        else:
            num_lines = countlines( src_path )

            #If the file exists, skip = 1
            if Path(tgt_path).is_file(): skip, method = True, 'a'
            else: skip, method = False, 'w'
                
            with open ( tgt_path, method ) as outfile:
                with open( src_path, 'r' ) as infile:
                    idx = 0
                    for line in infile:
                        if idx == 0 and skip == True : 
                            idx += 1
                        else: 
                            outfile.write(line)
                            idx += 1
                outfile.write('\n')       
                if skip == True: idx -= 1 
            print(f'\t   successfully appended {idx:<5} lines to:\n\t\t{tgt_path}\n ')

    except FileNotFoundError:
        print(f' ### ERROR >>> {file_tomove:<20}  was not found in {from_loc}')

    return num_lines, file_created


def main():
    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')

    ext_file = os.path.join(desktop,'file-locations.xlsx' )
    print(f'>>>> STARTING')
    print( f"=== Using information from: {os.path.basename( ext_file )}" )
    
    curwb = Worksheet()
    writewb = openpyxl.load_workbook( ext_file )
    cur_sheet = writewb.active
    
    curwb.xlsx_to_dict( ext_file ) 
    # print( f'HEADERS:  {curwb.header}' )   
    cols = []

    cols = curwb.header.keys()
    print(f'--KEYS--: {cols}\n-----------------------------------------------------------------------')
    #  dict_keys(['Filename', 'Source-path', 'Target-path', 'Concat-into', 'Create-date', 'Num-Lines', 'Move-date']) 
    # total_rows = 0
    # for total_rows, row in enumerate( curwb.sheet_items ):
    #     fn = row.get('Filename')
    #     print(f'{total_rows}: {fn}')
    #     total_rows += 1
    # print( f'=== TOTAL ENTRIES = {total_rows}' ) 

    for total_rows, row in enumerate( curwb.sheet_items ):    
        file_tomove = row.get('Filename')
        from_loc = row.get('Source-path')
        to_loc   = row.get('Target-path')
        created  = row.get('Create-date')
        combine2  = row.get('Concat-into')
        extn = '*'

        if combine2 == 'None':
            if  file_tomove.startswith('*'):
                if '.' in file_tomove: search,extn = file_tomove.split('.')
                file_created, num_lines  = copydirectory( from_loc, to_loc, extn )
            else:
                num_lines, file_created = copyfile( from_loc, to_loc, file_tomove, created )
        else:
            if  file_tomove.startswith('*'):
                if '.' in file_tomove: search,extn = file_tomove.split('.')
                file_created, num_lines  = combinedirectory( from_loc, to_loc, combine2, extn )            
            else:
                num_lines, file_created = combinefile( from_loc, to_loc, file_tomove, combine2, created )

        try:
            created_dt = datetime.datetime.strptime(created, '%Y-%m-%d %H:%M:%S')
        except:
            created_dt =  datetime.datetime.strptime('1901-01-01','%Y-%m-%d')

        if file_created and file_created > created_dt:
            file_str = str(file_created)[0:19]
            row['Create-date'] = file_str
            row['Num-Lines'] = num_lines
            if num_lines > 0:
                row['Move-date']= str(  datetime.datetime.now() )[0:19]
        # print(f'--> {total_rows}: {row}')

    #Update the Workbook and save
    try:
        for i, x in enumerate( curwb.sheet_items ):
            for idx,value in enumerate(x.values()):
                if value == 'None': value = ''
                cur_sheet.cell(row=i+2, column=idx+1).value = value
        writewb.save( ext_file )
    except PermissionError:
        print(f"Permission denied.  {ext_file} is currently open.")            
    except Exception as e:
        print( f'#### ERROR:  {e} ' )

if __name__ == '__main__':
    main()