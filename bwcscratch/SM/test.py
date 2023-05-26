from pathlib import Path

testfile= Path( 'c:/Temp/SM/file_name_with_underbars_details.json' )
print( str(testfile).split('\\')[-1])