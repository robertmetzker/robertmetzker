import csv, json
from datetime import datetime
from pathlib import Path

def set_libpath():
    r'''
    Set path import to be relative to the location of the dir the prog is run from
    C:\Users\nielsenjf\bwcroot\bwcenv\bwcrun\run_createviews.py
    becomes:  C:\Users\nielsenjf\bwcroot\
    '''
    import sys,os
    from pathlib import Path
    prog_path=Path(os.path.abspath(__file__))
    root=prog_path.parent.parent.parent.parent      # Extra parent added due to SM Sub-dir
    pyversion=f'{sys.version_info.major}{sys.version_info.minor}'
    
    pylibpath=root/f'Python/Python{pyversion}/site-packages'
    pylibpath2=root/f'bwcsetup/Python/Python{pyversion}/site-packages'
    sys.path.append(str(root))
    sys.path.append(str(pylibpath))
    sys.path.append(str(pylibpath2))
    print('using path',root,pylibpath)


set_libpath()

from bwcenv.bwclib import dblib,inf
from bwcsetup import dbsetup

def get_dbcon():
    env,db='dev','snow_me'  # change to DEV_ODS
    tgtdb = dbsetup.Envs[env][db]
    # con=dblib.DB(tgtdb,log=args.log,port=tgtdb.get('port',''))
    con=dblib.DB(tgtdb,log='',port=tgtdb.get('port',''))
    return con


# pass filenames containing the survey details and responses

def read_json_from_file(filename):
    with open(filename, encoding='utf-8', errors='ignore') as f:
        json_info = json.load(f)
    return json_info


def parse_survey(filepath,filename):
    survey_info=read_json_from_file(f'{filepath}/{filename}')
    surveys=survey_info['data']
    survey_list,survey_dict=[],{}
    for _idx,survey_info in enumerate(surveys):
        survey_dict['id']=survey_info.get('id')
        survey_dict['title']=survey_info.get('title').replace("\xa0","")
        temp=survey_dict.copy()
        survey_list.append(temp)
    return survey_list


def snow_list_stage(con,stage_dir):
    '''
        https://docs.snowflake.com/en/sql-reference/sql/list.html

    list @~/DBTEST/DBT_PBALZER/ACTIVITY_NAME_TYPE/;
    {'name': 'DEV_SOURCE/BASE/CARE824/CARE824.csv.gz', 'size': 61953552, 'md5': 'e59bcf86c48aad5c366bce6c4e6409d1', 'last_modified': 'Mon, 4 Oct 2021 19:56:44 GMT'}
    '''
    sql=f'list {stage_dir}; '

    stage_files=[row for  row in con.fetchdict(sql) ]
    return stage_files


def snow_remove_stage():
    '''
    rm @~/DBTEST/DBT_PBALZER/ACTIVITY_NAME_TYPE/ACTIVITY_NAME_TYPE.csv.gz;
    '''
    con = get_dbcon()
    stage_dir='@~/DEV_ODS/SURVEY_MONKEY/SURVEYS/'
    cmd_list=[]
    for row in snow_list_stage(con,stage_dir):
        remove_cmd=f"""rm @~/{row['name']}; """
        con.exe(remove_cmd)
        cmd_list.append(remove_cmd)
    print(f'---- Deleted {len(cmd_list)} files from {stage_dir} : {cmd_list}')
    print('='*80,'\n\n')    
    return cmd_list


def snow_put(path):
    '''
        requires the full path
    put file://I:\IT\ETL\nielsenjf\snowflake\extracts_active\ADR_TYP_INFSPLIT_2700000.gz @~/DBTEST/X10057301/ADR_TYP/ auto_compress=true;
    copy into X10057301.ADR_TYP from @~/DBTEST/X10057301/ADR_TYP/ file_format =  (type = csv field_delimiter = '\t' skip_header = 1)  on_error='continue';
    '''
    stage_dir='@~/DEV_ODS/SURVEY_MONKEY/SURVEYS/'
    if '\\' in path:
        fname = path.split('\\')[-1]
    else:
        fname = path.split('/')[-1]
        
    stage_cmd=f'''put file://{path} {stage_dir} auto_compress=true;'''
    print(f'\t{stage_cmd}')
    con=get_dbcon()
    result=con.exe(stage_cmd)
    staged_files=snow_list_stage(con,stage_dir)
    
    if not staged_files:
        raise Warning(f'Missing staged file: {path}')
    # print(f'Staged {staged_files}')

    table=path.split('_')[-1].split('.')[0]
    results=snow_copy_into(con,table,stage_dir, fname)

    if results.get('errors_seen',0) >0:
        print('*'*80,' ERROR ','*'*80)
        print(f'{results}\n','*'*167)
    else:
        print(f"\t\t:: LOADED: {results.get('rows_loaded',0) } rows\n\n" )
    return staged_files


def snow_copy_into(con,table,stage_dir, fname):
    #file_format=f"""file_format =  (type = csv field_delimiter = '{args.delim}' skip_header = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')  """
    #file_format="""FILE_FORMAT = '"DBTEST"."10057301"."BASIC_TSV"'"""

    # fname = fname+'.gz'
    if table == 'SURVEYS':
        print('Truncating SURVEYS table')
        con.exe('truncate table DEV_ODS.SURVEY_MONKEY.SURVEYS')
    copy_cmd=f"""copy into DEV_ODS.SURVEY_MONKEY.{table} from {stage_dir}{fname}.gz FILE_FORMAT='DEV_ODS.SURVEY_MONKEY.SURVEY_CSV' on_error='continue'; """
    # copy_cmd=f"""copy into DEV_ODS.SURVEY_MONKEY.{table} from {stage_dir} FILES = '{fname}.gz' FILE_FORMAT='DEV_ODS.SURVEY_MONKEY.SURVEY_CSV' on_error='continue'; """
    print(f'\t{copy_cmd}')
    result=list(con.exe(copy_cmd))
    print(f'\n\tCOPY INTO complete...')
    # print(f'\n\tCOPY INTO RESULTS: {str(result)}\n\n')
    return result[0]


def show_questions(detail_info):
    print('\n## Parsed Questions ##')
    print( f'TITLE:  {detail_info["title"]}' )
    print( f'QUESTIONS: {detail_info["question_count"]}' )
    print( f'CREATED: {detail_info["date_created"]}' )
    print('-'*20)
    for question in detail_info['pages'][0]['questions']:
        print( f'QUESTION ({question["position"]}): {question["id"]} : {question["headings"][0]["heading"]}' )

        if "other" in question.get("answers","None"):
            print( f'\t ANSWER: {question["answers"]["other"]["id"]} : {question["answers"]["other"]["text"]}' )

        try:
            for answer in question["answers"]["choices"]:
                print( f'\t ANSWER: {answer["id"]} : {answer["text"]}' )
        except:
                print( f'\t ESSAY:' )


def add_question_answers_to_dict(df: dict, detail_info: dict):
    survey_info = df.copy()

    for question in detail_info['pages'][0]['questions']:
        ans = {}
        qid = question["id"]
        survey_info[ qid ] = question["headings"][0]["heading"]
        # add an empty dictionary for each question to hold the answers based on anaswer id and answer text as key/value pairs
        # df[question["id"]]["answers"] = [{} for i in range(len(question["answers"]["choices"]))]

        if question["subtype"] != "essay":

            if "other" in question.get("answers","None"):
                ans_id = question["answers"]["other"]["id"]
                question_uni = question["answers"]["other"]["text"]
                question_str = question_uni.replace('0xA0','').replace('0x92','')
                ans[ans_id] = question_str

            if "choices" in question.get("answers","None"):
                try:
                    for answer in question["answers"]["choices"]:
                        ans[answer["id"]] = answer["text"]
                except:
                        ans[answer["id"]] = 'NO ANSWERS AVAILABLE'

            if "tag_data" in  question.get("answers","None"):
                answer_uni = answer["answers"][0]["text"] 
                answer_str = answer_uni.replace('0xA0','').replace('0x92','')
                ans[answer["id"]] = answer_str

        else:
            ans[answer["id"]] = 'Free Form Field'

        # Add the ans dictionary to  dictionary within df[question["id"]]["answers"]
        # df[question["id"]]["answers"] = ans
        survey_info.update(ans)

        # print(survey_info )
    return survey_info 


def show_responses(response_data, parsed_questions):
    # print( response_data )
    print('Recipent: ', response_data["recipient_id"]) 
    print('FName: ', response_data["first_name"]) 
    print('LName: ', response_data["last_name"]) 
    if response_data["metadata"]["contact"].get("email",False):
        print('Email: ', response_data["metadata"]["contact"]["email"]["value"]) 
    else:
        print("Email:  N/A")

    print('DATE_CREATED: ', response_data["date_created"]) 

    print('DATE_MODIFIED: ', response_data["date_modified"]) 
    print('TIME_TAKEN: ', response_data["total_time"]) 

    print('SurveyID: ', response_data["survey_id"]) 
    # print('Answers: ', response_data[0]["pages"][0]["questions"]) 

    for answer in response_data["pages"][0]["questions"]:
        qid = answer["id"]
        aid = answer["answers"][0].get("choice_id",'Free Form Field')
        print( f'QUESTION: {qid} >  {parsed_questions[qid]}' )
        if "tag_data" in answer["answers"][0]:
            aid = 'Free Form'
            try:
                print( f'  ANSWER: {aid} ** > {answer["answers"][0]["text"]}' )
            except:
                print( f'{answer}')
        if aid != 'Free Form':
            print( f'  ANSWER: {aid}>  {parsed_questions.get(aid)}' )


def add_response_to_dict(response_info: dict, response_data: dict):
    # { 'recipent':'6070975071' ,'fname':'1599387', 'lname':'TGW-SYSTEMS INC', 'email':'laura.butrick@tgw-group.com', 'surveyid':'300747984', 'responses': {'605148537':'3982038472', '605148539':'3982038484', '605148542':'3982038498', '605148543':'3982038503', '605148544':'3982038508', '605148545':'3982038511', '605148546':'3982038514', '605148547':'3982038520', '605148548':'3982038523', '605148549':'3982038528', '605148550':'3982038534'} }
    df = response_info.copy()

    df["person"] = response_data["recipient_id"]
    d=response_data["first_name"]
    
    fmt = ['%m/%d/%Y','%b %d, %Y']
    for f in fmt:
        try:
            new = datetime.strptime(d, f).strftime('%Y-%m-%d')
            break
        except:
            new = d

    df["fname"] = new
    # df["fname"] = response_data["first_name"]
    df["lname"] = response_data["last_name"]
    if response_data["metadata"]["contact"].get("email",False):
        df["email"] = response_data["metadata"]["contact"]["email"]["value"]
    else:
        df["email"] = "N/A"
    df["surveyid"] = response_data["survey_id"]
    df["surveydate"] = response_data["date_created"]
    df["totaltime"] = response_data["total_time"]
    df["dtcreated"] = response_data["date_created"]
    df["dtmodified"] = response_data["date_modified"]
    

    response_dict = {}
    for answer in response_data["pages"][0]["questions"]:
        qid = answer["id"]
        aid = answer["answers"][0].get("choice_id","None")
        if "tag_data" in answer["answers"][0]:
            ans_str = answer["answers"][0]["text"]
            # ans_str = ans_str.replace('0xA0',' ').replace('0x92',"'")
            ans_str = ans_str.encode('ascii','ignore')
            ans_str = ans_str.decode()
            response_dict[qid] = ans_str       
        else:
            response_dict[qid] = aid
    
    df["responses"] = response_dict
    # print(df)
    return df


def output_questions( detail_info ):
    '''
    {"id": "605148537", "position": 1, "visible": true, "family": "single_choice", "subtype": "vertical", "layout": {"bottom_spacing": 0, "col_width": null, "col_width_format": null, "left_spacing": 0, "num_chars": 50, "num_lines": 1, "position": "new_row", "right_spacing": 0, "top_spacing": 0, "width": null, "width_format": null}, "sorting": null, "required": null, "validation": null, "forced_ranking": false, "headings": [{"heading": "What is your relationship to the employer that went through the premium audit or rating inspection process?"}], "href": "https://api.surveymonkey.com/v3/surveys/300747984/pages/154390018/questions/605148537", "answers": {"other": {"position": 0, "visible": true, "text": "Other (please specify)", "id": "3982038475", "num_lines": 1, "num_chars": 50, "is_answer_choice": true, "apply_all_rows": false, "error_text": "Please enter a comment."}, "choices": [{"position": 1, "visible": true, "text": "Employee of company", "quiz_options": {"score": 0}, "id": "3982038472"}, {"position": 2, "visible": true, "text": "CPA/Accounting firm", "quiz_options": {"score": 0}, "id": "3982038473"}, {"position": 3, "visible": true, "text": "Third Party Administrator", "quiz_options": {"score": 0}, "id": "3982038474"}]}}, 

    SURVEY_ID       text,
    QUESTION_NO     number,
    QUESTION_ID     text,
    QUESTION_TXT    text
    '''
    question_headers = "SURVEY_ID,QUESTION_NO,QUESTION_ID,QUESTION_TXT"
    questions_str = []
    surveyid = detail_info['id']

    for question in detail_info['pages'][0]['questions']:
        qid = question["id"]
        qno = question["position"]
        qtxt =  question["headings"][0]["heading"]
        # qtxt =  question["headings"][0]["heading"].replace("'","''") 
        # sql_tr =  f"insert into SURVEY_MONKEY.QUESTIONS values( {surveyid!r}, {qno},  {qid!r},'{qtxt}' );\n" 
        str =  f'{surveyid},{qno},{qid},"{qtxt}"\n' 
        # str = str.replace('0xA0',' ').replace('0x92',"'")
        str = str.encode('ascii','ignore')
        str = str.decode()
        questions_str.append( str )

    return question_headers, questions_str


def output_answers( detail_info ):
    '''
    {"other": {"position": 0, "visible": true, "text": "Other (please specify)", "id": "3982038475", "num_lines": 1, "num_chars": 50, "is_answer_choice": true, "apply_all_rows": false, "error_text": "Please enter a comment."}, "choices": [{"position": 1, "visible": true, "text": "Employee of company", "quiz_options": {"score": 0}, "id": "3982038472"}, {"position": 2, "visible": true, "text": "CPA/Accounting firm", "quiz_options": {"score": 0}, "id": "3982038473"}, {"position": 3, "visible": true, "text": "Third Party Administrator", "quiz_options": {"score": 0}, "id": "3982038474"}]}

    SURVEY_ID       text,
    QUESTION_ID     text,
    ANSWER_ID       text,
    ANSWER_TXT      text
    '''
    answer_headers = "SURVEY_ID,QUESTION_ID,ANSWER_ID,ANSWER_TXT"
    answers_str = []
    surveyid = detail_info['id']

    for question in detail_info['pages'][0]['questions']:
        qid = question["id"]
        if "other" in question.get("answers","None"):
            aid = question["answers"]["other"]["id"]
            atxt = question["answers"]["other"]["text"]

        try:
            for answer in question["answers"]["choices"]:
                aid = answer["id"]
                atxt = answer["text"]
                atxt = atxt.replace("\n"," ").replace("\r"," ")

                # atxt = atxt.replace("'","''").replace("\n"," ").replace("\r"," ")
                # sql_str =  f"insert into SURVEY_MONKEY.ANSWERS values(  {surveyid!r}, {qid!r},  {aid!r},{atxt!r} );\n"
                str =  f'{surveyid},{qid},{aid},"{atxt}"\n'
                # str = str.replace('0xA0',' ').replace('0x92',"'")
                str = str.encode('ascii','ignore')
                str = str.decode()
                answers_str.append( str )
        except:
            pass
            # Possible to not have an answer, and if so, parse the other field
            # str = f"insert into SURVEY_MONKEY.ANSWERS values(  {surveyid!r}, {qid!r},  {aid!r},{atxt!r} );\n" 
            # answers_str.append( str )

        if "other" in question.get("answers","None"):
            aid = question["answers"]["other"]["id"]
            atxt = question["answers"]["other"]["text"]
            atxt = atxt.replace("\n"," ").replace("\r"," ")
            # atxt = atxt.replace("'","''")
            # sql_str =  f"insert into SURVEY_MONKEY.ANSWERS values(  {surveyid!r}, {qid!r},  {aid!r},{atxt!r} );\n" 
            str =  f'{surveyid},{qid},{aid},"{atxt}"\n'
            # str = str.replace('0xA0',' ').replace('0x92',"'")
            str = str.encode('ascii','ignore')
            str = str.decode()
            answers_str.append( str )

    return answer_headers, answers_str


def output_participants(all_responses):
    '''
    [{'recipent': '6070975071', 'fname': '1599387', 'lname': 'TGW-SYSTEMS INC', 'email': 'laura.butrick@tgw-group.com', 'surveyid': '300747984', 'surveydate': '2021-02-02T19:32:47+00:00', 'totaltime': 88, 'responses': {'605148537': '3982038472', '605148539': '3982038484', '605148542': '3982038498', '605148543': '3982038503', '605148544': '3982038508', '605148545': '3982038511', '605148546': '3982038514', '605148547': '3982038520', '605148548': '3982038523', '605148549': '3982038528', '605148550': '3982038534'}},

    PARTICIPANT_ID  text,
    FIRST_NAME      text,
    LAST_NAME       text,
    EMAIL           text
    '''
    participant_headers = "PARTICIPANT_ID,FIRST_NAME,LAST_NAME,EMAIL,DATE_CREATED,DATE_MODIFIED,TIME_TAKEN"
    stmt = 'insert into SURVEY_MONKEY.PARTICIPANTS values ('
    participant_str = []
    
    for person in all_responses:
        person_dict = dict( person )
        # sql_str = f"{stmt} {person_dict.get('person')!r}, {person_dict.get('fname')!r}, {person_dict.get('lname')!r}, {person_dict.get('email')!r} );\n"
        str = f"{person_dict.get('person')},{person_dict.get('fname')},{person_dict.get('lname')},{person_dict.get('email')},{person_dict.get('dtcreated')},{person_dict.get('dtmodified')},{person_dict.get('totaltime')}\n"
        participant_str.append( str )

    return participant_headers, participant_str


def output_responses(all_responses):
    '''
    [{'person': '7565351911', 'fname': '244198', 'lname': 'ROMPS WATER PORT INC', 'email': 'office@romps.com', 'surveyid': '300747984', 'surveydate': '2023-02-08T20:27:09+00:00', 'totaltime': 75, 'responses': {'605148537': '3982038472', '605148539': '3982038483', '605148542': '3982038499', '605148543': '3982038504', '605148544': '3982038508', '605148545': '3982038511', '605148546': '3982038515', '605148547': '3982038519', '605148548': '3982038523', '605148549': '3982038529', '605148550': '3982038534'}}]

    SURVEY_ID       text,    --300747984
    PARTICIPANT_ID  text,
    QUESTION_ID     text,
    ANSWER_ID       text    -- May also be free form text
    '''
    response_str = []
    response_headers = "SURVEY_ID,PARTICIPANT_ID,QUESTION_ID,ANSWER_ID"

    for person in all_responses:
        try:
            person_dict = dict( person )
            ans_list = person_dict.get('responses',"")
            for q,a  in  ans_list.items() :
                ans = a
                if not a: ans= 'None'
                #replace all single quotes with two single quotes
                ans = ans.replace("\n"," ").replace("\r", " ").replace('"',"'")

                # Replace all non-printable ASCII with a space
                ans = ''.join([i if ord(i) < 128 else ' ' for i in ans ])
                # ans = ans.replace("'","''").replace("\n"," ").replace("\r", " ")
                # sql_str = f"{stmt} {person_dict.get('surveyid')!r}, {person_dict.get('person')!r}, {q!r}, '{ans}' );\n"
                str = f"{person_dict.get('surveyid')},{person_dict.get('person')},{q},\"{ans}\"\n"
                response_str.append( str )
        except:
            print( f'##### ERROR:  {person}')

    return response_headers, response_str


def writecsv( func, headers, filepath, results ):
    schema_name = 'SURVEY_MONKEY'
    filename = f"{filepath}\output\{schema_name}_{func}.csv"

    # write_csv(fname,rows,raw=False,delim='\t',term='\n',prefix='',sortit=True,log=None,verify=False):
    with open( filename, 'w', newline = '') as file2write:
        file2write.write( f'{headers}\n')
        for row in results:
            file2write.write( row )
            
    print( f' --- Wrote {func} CSV file to: {filename}\n')
    snow_put(filename)


def sm_schema_reset():
    print('Truncating all Survery Monkey tables')
    con=get_dbcon()
    tables=['QUESTIONS','ANSWERS','PARTICIPANTS','RESPONSES','SURVEYS']
    for t in tables:
        sql=f'truncate table DEV_ODS.SURVEY_MONKEY.{t}'
        con.exe(sql)
        print(sql)
    print('='*80)


def main():
    sm_schema_reset()

    # create a list of Survey objects
    filepath = Path( 'I:/EDM/sm/2023_04_18PM/extracts' )
    outputdir = filepath / 'output'
    #print survey titles

    filelist=  list( filepath.glob( "*_details.json" ) )
    # surveys = [ Survey(**survey) for survey in all_surveys['data'] ]
    surveys = parse_survey(filepath,'surveys.json')
    survey_sql = []
    survey_headers = "SURVEY_ID,SURVEY_NAME"
    # survey_sql.append( f'--SURVEYS available: {len(surveys)} --\n')

    for survey in surveys:
        # sql_str =  f"insert into SURVEY_MONKEY.SURVEYS values ( {survey.id!r}, '{survey.title}' );\n"
        str =  f'''{survey['id']},"{survey['title']}"\n'''
        survey_sql.append( str )

    snow_remove_stage()

    for filename in filelist:
        detail_file = filename.name
        response_file = detail_file.replace("detail","response")
        
        print( f'--- Reading survey details from {detail_file} ---' )
        detail_info = read_json_from_file( filepath/detail_file )

        print( f'--- Reading response information from {response_file} ---' )
        response_info = read_json_from_file( filepath/response_file )

        survey_info = {}

        survey_info['surveyid'] = detail_info['id']
        survey_info[detail_info['id']] = detail_info['title']
        survey_info['question_count'] = detail_info['question_count']

        survey_info['date_created'] = detail_info['date_created']
        
        # show_questions( detail_info )
        question_headers, question_sql = output_questions( detail_info )
        parsed_questions = add_question_answers_to_dict(survey_info, detail_info)
        # print( parsed_questions )

        answer_headers, answer_sql = output_answers( detail_info )

        print(f'\n\n//- Parsing example response data --')
        # print( len( response_info));input("!!!!!")      # 40 pages
        response_dict = {}
        all_responses = []

        try:
            # Loop through and process each page of responses
            print('\n-- Converting response to dictionary')

            for response_set in response_info:
                # all_response_data = response_info[0]['data']
                all_response_data = response_set['data']
                # Test a single response
                # response_data = all_response_data[1]

                tot_responses = len(all_response_data)
                for idx in range(tot_responses):
                    response_data = all_response_data[idx]
                    response_dict = add_response_to_dict(response_dict, response_data)

                for response in  all_response_data:
                    response_dict = add_response_to_dict(response_dict, response)
                    all_responses.append(response_dict)

            # print(all_responses)
            participant_headers, participant_sql = output_participants(all_responses)
            response_headers, response_sql = output_responses(all_responses)

            output_file = detail_file.rsplit("_",1)[0]

            writecsv( f'{output_file}_QUESTIONS', question_headers,filepath, question_sql )
            writecsv( f'{output_file}_ANSWERS', answer_headers,filepath, answer_sql )
            writecsv( f'{output_file}_PARTICIPANTS', participant_headers,filepath, participant_sql )
            writecsv( f'{output_file}_RESPONSES', response_headers,filepath, response_sql )
        
        except:
            print( f'!!! ERR: Unable to process: {filename}')
            
    writecsv( f'CURRENTLY_ACTIVE_SURVEYS', survey_headers,filepath,survey_sql )


if __name__ == "__main__":
    main()
    
