import sys,argparse,os
from pathlib import Path
from bwcenv.bwclib import dblib,inf,sflib
from bwcsetup import dbsetup
from datetime import datetime
import random
import csv

def set_libpath():
    prog_path=Path(os.path.abspath(__file__))
    libpath=str(prog_path.parent.parent.parent)
    sys.path.append(libpath)

def process_args():
    etldir =f"I:/Data_Lake/IT/ETL/{os.environ['USERNAME'].replace('_x','')}/EL/"
    parser = argparse.ArgumentParser(description='Hello',epilog='hello again')
    parser.add_argument('--sql',required=False, help='default directory for all logs, data files')
    parser.add_argument('--etldir', default=etldir,help='default directory for all logs, data files')
    args= parser.parse_args()
    args.etldir=Path(args.etldir)
    args.logdir=args.etldir/'logs'
    args.log=inf.setup_log(args.logdir,app='parent')
    args.log.info(f'processing in {args.etldir}')
    args.vt_creds='/uat2/vertica_me'
    args.env,args.vt_db=args.vt_creds.split('/')[1:]
    args.sf_creds='snowflake_prod'
    args.sf_db=args.sf_creds
    return args
def vt_check(args):
    args.log.info('checking vertica connection')
    tgtdb = dbsetup.Envs[args.env][args.vt_db]
    con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    args.log.info('vertica connected')
    return con

def get_validation_queries(args, sfCur, vtCon, ValidationQueryTable, ValidationResultsTable, sf_results_writer):
    queries = sfCur.execute(f'SELECT VALIDATION_QUERY_ID,SF_SQL_QUERY,VERTICA_SQL_QUERY from {ValidationQueryTable};').fetchall()
    queries_ct = sfCur.execute(f'SELECT count(*) from {ValidationQueryTable};').fetchone()
    print(type(queries))
    args.log.info('----------------------------------------------------------')
    args.log.info(f'{queries}')
    args.log.info('----------------------------------------------------------')
    args.log.info('----------------------------------------------------------')
    # args.log.info('Total Number of Validations = %s' , query_ct)
    args.log.info('----------------------------------------------------------')
    for VALIDATION_QUERY_ID,SF_SQL_QUERY,VERTICA_SQL_QUERY in queries:
        print(VALIDATION_QUERY_ID, SF_SQL_QUERY, VERTICA_SQL_QUERY)
        run_validation(args, sfCur, vtCon, VALIDATION_QUERY_ID, SF_SQL_QUERY, VERTICA_SQL_QUERY, ValidationResultsTable,sf_results_writer)

    print(queries_ct[0])
    print(queries[0])
    # if queries_ct[0] > 1:
    #     parallel =2
    # else: 
    #     parallel =1
    
    # if args.parallel==1: 
    #      args.log.info('running in single threaded mode')
    #      for allarg in all_args: run_validation(allarg)
    # else:
    #     args.log.info('running in parallel mode')
    #     results=inf.run_parallel(run_validation,args=all_args,parallel=args.parallel)
    #     print(results)

    # print("Validations Completed -- Going to main")

def run_validation(args, sfCur, vtCon, VALIDATION_QUERY_ID, SF_SQL_QUERY, VERTICA_SQL_QUERY, ValidationResultsTable, sf_results_writer):
    val_qid = VALIDATION_QUERY_ID
    # try:
    #     sf_data = sfCur.execute(SF_SQL_QUERY)
    #     data = sf_data.fetchall()
    #     counts = [data[r][0] for r in range(len(data))] 
    #     sf_count = counts[0]
    #     print('********************** sfcount:' , sf_count)
    #     print('********************** sfcount:' , type(sf_count))
    #     sf_status = True
    # except:
    #     args.logerror('Failed Validation for Query ID -- Snowflake: %s', VALIDATION_QUERY_ID)
    #     args.logerror('SF Query : %s',SF_SQL_QUERY)
    sf_status = False
    sf_count = 0
    try: 
        vt_dic = vtCon.fetchone(VERTICA_SQL_QUERY)
        vt_count = vt_dic.get('COUNT')
        vt_status = True
    except:
        args.logerror('Failed Validation for Query ID -- Vertica: %s', VALIDATION_QUERY_ID)
        args.logerror('SF Query : %s',VERTICA_SQL_QUERY)
        vt_count = 0
        vt_status = False

    send_result(args, sfCur, ValidationResultsTable, sf_count, vt_count, val_qid, sf_status, vt_status, sf_results_writer)

def send_result(args, sfCur, ValidationResultsTable, sf_count, vt_count, val_qid, sf_status, vt_status, sf_results_writer):
    sf_results_data = [val_qid,sf_status,sf_count,vt_status,vt_count, datetime.now()]
    sf_results_writer.writerow(sf_results_data)
    insrt_sql = '''insert into ''' + ValidationResultsTable + ''' (RUN_ID,VALIDATION_QUERY_ID, SF_SQL_STATUS, SF_SQL_RESULT, VERTICAL_SQL_STATUS, VERTICA_SQL_RESULT, RUN_TIME) 
                    VALUES (''' +str(random.randint(1000000, 100000000)) + ''', 
                    ''' + str(val_qid) + ''', 
                    ''' + str(sf_status) + ''', 
                    ''' + str(sf_count) + ''', 
                    ''' + str(vt_status) + ''', 
                    ''' + str(vt_count) + ''',CURRENT_TIMESTAMP())'''
    sfCur.execute(insrt_sql)
    args.log.info('Result Added to Validation_Result for Query ID : %s', val_qid)

def generate_results_file():
    now = datetime.now()
    dt_string = now.strftime("%d%m%Y_%H%M%S")
    header = ['VALIDATION_QUERY_ID','SF_SQL_STATUS','SF_SQL_RESULT','VERTICAL_SQL_STATUS','VERTICA_SQL_RESULT','RUN_TIME']
    path = r'C:\Users\a85358'
    results_file_name = 'sf_results_' + dt_string + '.csv'
    sf_w = open(os.path.join(path,results_file_name), 'w', encoding='UTF8', newline='')
    writer = csv.writer(sf_w)  
    writer.writerow(header)
    return writer


def main():
    set_libpath()
    args=process_args()
    args.log.info('Starting Validations Program . .')
    vtCon = vt_check(args)
    sfCon = sflib.sf_connection(args, dbsetup.Envs[args.env][args.sf_db])
    sfCur = sfCon.cursor()
    ValidationQueryTable = 'validation_queries'
    ValidationResultsTable = 'validation_results'
    sf_results_writer = generate_results_file()
    get_validation_queries(args, sfCur, vtCon, ValidationQueryTable, ValidationResultsTable, sf_results_writer)
    args.log.info('SuccessFully Completed All Validations')

if __name__=='__main__':
    main()