#stds libs
import zipfile, os, datetime, csv, argparse, types, warnings
from pathlib import Path

warnings.filterwarnings('ignore')

#other libs

from github import Github #pip install PyGithub

import requests #pip install Requests

#local libs
def set_libpath():
    r'''
    Set path import to be relative to the location of the dir the prog is run from
     C:\Users\nielsenjf\bwcroot\bwcenv\bwcrun\run_createviews.py
    becomes:  C:\Users\nielsenjf\bwcroot\
    '''
    import sys,os
    from pathlib import Path
    prog_path=Path(os.path.abspath(__file__))
    root=prog_path.parent.parent.parent
    pyversion=f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    print('using path',root,pylibpath)

set_libpath()
from bwcenv.bwclib import inf

from bwcsetup import gh_api
token=gh_api.token

#local libs
def github_download(args,release_url,tgtzip):
    session = requests.Session();session.verify=False
    headers = {'Authorization':'token '+args.token,'Accept':'application/octet-stream'}

    response= session.get(release_url,stream =True, headers = headers)

    with open(tgtzip,'wb') as fw:
        for block in response.iter_content(1024*1024):
            fw.write(block)
        print('Finished Extracting...' )
        
    
def compare_info(args, latest_info ):
    print( f'-- LATEST INFO:\n{latest_info}\n\n')
    args.srczip.parent.mkdir(parents=True,exist_ok=True)
    release_info_file = args.srczip.parent/'latest_release.txt'

    # if os.path.exists( 'c:/temp/latest_release.txt'):
    if release_info_file.exists():
        with open( release_info_file,'r') as f:
            prev_release = f.read()
            print( f'---PREVIOUSLY:\n{prev_release}\n\n')
    else:
        prev_release = 'none'
        
    if prev_release == latest_info:
        print( '### Currently already on the latest Release ###' )
        return 'ON LATEST'
    else:
        print( f'The latest information saved to:  c:/temp/latest_release.txt\n')
        return 'NOT LATEST'    


def get_release_url(args):
    '''
    returns the url from github that is the zip file for a release
    https://github.com/BWC-DW/repo-dbt-snowflake/archive/refs/tags/20211014_UAT2_Release.zip
    https://github.com/BWC-DW/sandbox/archive/refs/tags/test.zip
    '''
    this_release = args.repo.get_latest_release()
    baseurl=f'https://github.com/{args.repo_name}/archive/refs/tags'
    
    # print(release)
    latest_info =  f'\tTITLE: {this_release.title}\n\t BODY: {this_release.body}\n\t  TAG: {this_release.tag_name}\n RELEASED: {this_release.last_modified}'
    comparison = compare_info(args, latest_info ) 

    release_info_file = args.srczip.parent/'latest_release.txt'

    if comparison == 'NOT LATEST':
        final_url = f'{baseurl}/{this_release.tag_name}.zip'
        with open( release_info_file,'w') as f:
            f.write( latest_info )
    else:
        final_url = 'ON LATEST'

    return final_url


def bkpfldr(fnltestdir, bkpdir):
    '''
    test > bkdir/test_ymdhms
    'srczip' :'C:/Users/a75551/deploy/test.zip'
    'tgtdir': WindowsPath('C:/Users/a75551/deploy'
    'fnltestdir': WindowsPath('C:/Users/a75551/deploy/test'
    'bkpdir': WindowsPath('C:/Users/a75551/deploy/bkpdir'
    '''
    now=datetime.datetime.now()
    ymdhms=now.strftime('%Y%m%d-%H%M%S')
    fldrname=fnltestdir.name+'_'+ymdhms

    fnltestdir.replace(bkpdir/fldrname)
    print(f'=== Replaced folder: {fnltestdir}\n\twith folder: {bkpdir/fldrname} ')
    # print(locals())


def unzip2dir(srczip,release_name,tgtdir='',bkpdir='',bkup=True):
    if not srczip.exists():
        print(f'# ERROR: Zip file {srczip} does not exist. Returning')
        return

    if not tgtdir:
        #tgtdir inherits the methods and properties from srczip
        tgtdir=srczip.parent

    if not bkpdir:
        #addition of folder bkpdir to tgtdir
        bkpdir=tgtdir/'bkpdir'
    
    if bkup:
        #create bkdir if it does not exist
        bkpdir.mkdir(parents=True, exist_ok=True)

    #returns 
    fnltestdir=tgtdir/srczip.stem
    print('fnltestdir = ',fnltestdir)
    if fnltestdir.exists():
        print(f'Backing up... {fnltestdir} present for backup')
        bkpfldr(fnltestdir, bkpdir)
    else:
        print(f'Skipping... Previous {fnltestdir} not present for backup')

    with zipfile.ZipFile(srczip) as zipobj:
        # zipobj.printdir()
        ziplist = zipobj.namelist()
        ziproot = ziplist[0]
        # zipobj.extract(zipdir, tgtdir)
        zipobj.extractall(tgtdir)
        extracted=tgtdir/ziproot
        print(f'Folder: {zipobj}\n\textracted to: {tgtdir}')

    extracted.rename(tgtdir/release_name)


def process_args():
    '''
    python run_deply.py --repo sandbox --srczip c:/temp/release.zip
    python run_deply.py --repo repo-dbt-snowflake --srczip E:/Data_Lake/IT/ETL/Releases/repo-dbt-snowflake/release.zip
    '''
    parser = argparse.ArgumentParser(description='test args')


    #required
    parser.add_argument( '--repo', required=True, help='The Repository',)
    parser.add_argument( '--srczip', required=True, help='The Source Zip File',)
    
    #optional
    parser.add_argument( '--debug', required=False, help='The Debug',)
    parser.add_argument( '--org', required=False, default='BWC-DW', help='Organization')

    #boolean

    args = parser.parse_args()
    args.repo_name = f'{args.org}/{args.repo}'

    args.srczip=Path(args.srczip)
    args.release_name=args.srczip.stem
    
    args.token=token
    args.github=Github(args.token,verify=False)

    return args


def main():
    print('=== Starting')
    args=process_args()
    print(args)
    args.repo = args.github.get_repo( args.repo_name )

    release_url=get_release_url( args )

    if release_url != 'ON LATEST': 
        github_download( args, release_url, args.srczip )
        unzip2dir( args.srczip, args.release_name)

    print('=== Done' )

if __name__ == '__main__':
    main()

'''
python run_deploy.py --repo sandbox --srczip c:/temp/release.zip
python run_deploy.py --repo repo-dbt-snowflake --srczip E:/Data_Lake/IT/ETL/Releases/repo-dbt-snowflake/release.zip
'''
