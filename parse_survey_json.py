import csv, json
from pathlib import Path
from dataclasses import dataclass, field

all_surveys = {"data": [
    {"id": "300747984", "title": "Ohio Bureau of Workers' Compensation Employer Audit Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/300747984"}, 
    {"id": "303452492", "title": "Ohio BWC Employer Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/303452492"}, 
    {"id": "112758772", "title": "IW Pulse Survey - 30 Day", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/112758772"}, 
    {"id": "301195553", "title": "IW Pulse Survey - 90 Day", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/301195553"}, 
    {"id": "401765993", "title": "IW Pulse Survey - Medical Only", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/401765993"}, 
    {"id": "312123669", "title": "Ohio BWC First Report of Injury (FROI) Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/312123669"}, 
    {"id": "318130682", "title": "Ohio BWC MCO Customer Satisfaction Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/318130682"}, 
    {"id": "316723735", "title": "January 2022 Workshop Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/316723735"}, 
    {"id": "313524368", "title": "Workshop Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/313524368"}, 
    {"id": "314252541", "title": "Claim Topics for Targeted Messaging System", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/314252541"}, 
    {"id": "163761262", "title": "Ohio Bureau of Workers' Compensation Employer Audit Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/163761262"}, 
    {"id": "128705841", "title": "Ohio BWC MCO Customer Satisfaction Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/128705841"}]
, "per_page": 50, "page": 1, "total": 12, "links": {"self": "https://api.surveymonkey.com/v3/surveys?per_page=50&page=1"}
}

response_data = [{"id": "12376448666", "recipient_id": "6070975071", "collection_mode": "default", "response_status": "completed", "custom_value": "Jordan, S", "first_name": "1599387", "last_name": "TGW-SYSTEMS INC", "email_address": "", "ip_address": "107.4.69.163", "logic_path": {}, "metadata": {"contact": {"email": {"type": "string", "value": "laura.butrick@tgw-group.com"}, "first_name": {"type": "string", "value": "1599387"}, "last_name": {"type": "string", "value": "TGW-SYSTEMS INC"}, "custom_value": {"type": "string", "value": "Jordan, S"}, "custom_value2": {"type": "string", "value": "1/29/2021"}, "custom_value3": {"type": "string", "value": "438765"}, "custom_value4": {"type": "string", "value": "1/29/2021"}}}, "page_path": [], "collector_id": "399901743", "survey_id": "300747984", "custom_variables": {}, "edit_url": "https://www.surveymonkey.com/r/?sm=WbOxayZCcCXe7Fjdxfn81xe2XgJs2w8FmFrRNBzX0F_2BhQpe4F6U3Svjg2HgHwbn5", "analyze_url": "https://www.surveymonkey.com/analyze/browse/xDwfKEeYKlwB67d7Bs_2FYnO8R1V931r1XjG0Ezz12s0g_3D?respondent_id=12376448666", "total_time": 88, "date_modified": "2021-02-02T19:34:16+00:00", "date_created": "2021-02-02T19:32:47+00:00", "href": "https://api.surveymonkey.com/v3/surveys/300747984/responses/12376448666", "pages": [{"id": "154390018", "questions": [{"id": "605148537", "answers": [{"choice_id": "3982038472"}]}, {"id": "605148539", "answers": [{"choice_id": "3982038484"}]}, {"id": "605148542", "answers": [{"choice_id": "3982038498"}]}, {"id": "605148543", "answers": [{"choice_id": "3982038503"}]}, {"id": "605148544", "answers": [{"choice_id": "3982038508"}]}, {"id": "605148545", "answers": [{"choice_id": "3982038511"}]}, {"id": "605148546", "answers": [{"choice_id": "3982038514"}]}, {"id": "605148547", "answers": [{"choice_id": "3982038520"}]}, {"id": "605148548", "answers": [{"choice_id": "3982038523"}]}, {"id": "605148549", "answers": [{"choice_id": "3982038528"}]}, {"id": "605148550", "answers": [{"choice_id": "3982038534"}]}]}]},]

# create a dataclass to hold the survey data
@dataclass
class Survey:
    id: str
    title: str
    nickname: str
    href: str                                                       # Base URL for the survey
    detail_url: str = field(init=False) 
    responses_url: str = field(init=False) 

    def __post_init__(self):
        self.detail_url = self.href + '/details'                    # Details url containing the questions and answera available for the survey
        self.responses_url = self.href + '/responses/bulk'          # Responses url containing the responses for the survey
        # self.title = self.title.replace("'","''")



# pass filenames containing the survey details and responses

def read_json_from_file(filename):
    with open(filename) as f:
        json_info = json.load(f)
    return json_info


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
                ans[ans_id] = question["answers"]["other"]["text"]

            if "choices" in question.get("answers","None"):
                try:
                    for answer in question["answers"]["choices"]:
                        ans[answer["id"]] = answer["text"]
                except:
                        ans[answer["id"]] = 'NO ANSWERS AVAILABLE'

            if "tag_data" in  question.get("answers","None"):
                ans[answer["id"]] = answer["answers"][0]["text"] 

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
    print('Email: ', response_data["metadata"]["contact"]["email"]["value"]) 

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
    df["fname"] = response_data["first_name"]
    df["lname"] = response_data["last_name"]
    df["email"] = response_data["metadata"]["contact"]["email"]["value"]
    df["surveyid"] = response_data["survey_id"]
    df["surveydate"] = response_data["date_created"]
    df["totaltime"] = response_data["total_time"]

    response_dict = {}
    for answer in response_data["pages"][0]["questions"]:
        qid = answer["id"]
        aid = answer["answers"][0].get("choice_id","None")
        if "tag_data" in answer["answers"][0]:
            response_dict[qid] = answer["answers"][0]["text"]            
        else:
            response_dict[qid] = aid
    
    df["responses"] = response_dict
    # print(df)
    return df


def output_questions( detail_info ):
    question_headers = "SURVEY_ID,QUESTION_NO,QUESTION_ID,QUESTION_TXT"
    questions_str = []
    surveyid = detail_info['id']

    for question in detail_info['pages'][0]['questions']:
        qid = question["id"]
        qno = question["position"]
        qtxt =  question["headings"][0]["heading"]
        # qtxt =  question["headings"][0]["heading"].replace("'","''") 
        # sql_tr =  f"insert into SURVEY_MONKEY.QUESTIONS values( {surveyid!r}, {qno},  {qid!r},'{qtxt}' );\n" 
        str =  f'{surveyid!r},{qno},{qid!r},"{qtxt}"\n' 
        questions_str.append( str )

    return question_headers, questions_str


def output_answers( detail_info ):
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
                str =  f'{surveyid!r},{qid!r},{aid!r},"{atxt}"\n'
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
            str =  f'{surveyid!r},{qid!r},{aid!r},"{atxt}"\n'
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
    participant_headers = "PARTICIPANT_ID,FIRST_NAME,LAST_NAME,EMAIL"
    stmt = 'insert into SURVEY_MONKEY.PARTICIPANTS values ('
    participant_str = []
    
    for person in all_responses:
        person_dict = dict( person )
        # sql_str = f"{stmt} {person_dict.get('person')!r}, {person_dict.get('fname')!r}, {person_dict.get('lname')!r}, {person_dict.get('email')!r} );\n"
        str = f"{person_dict.get('person')!r},{person_dict.get('fname')!r},{person_dict.get('lname')!r},{person_dict.get('email')!r}\n"
        participant_str.append( str )

    return participant_headers, participant_str


def output_responses(all_responses):
    '''
    SURVEY_ID       text,    --300747984
    PARTICIPANT_ID  text,
    QUESTION_ID     text,
    ANSWER_ID       text    -- May also be free form text
    '''
    response_str = []
    response_headers = "SURVEY_ID,PARTICIPANT_ID,QUESTION_ID,ANSWER_ID"

    for person in all_responses:
        #{'person': '7565351911', 'fname': '244198', 'lname': 'ROMPS WATER PORT INC', 'email': 'office@romps.com', 'surveyid': '300747984', 'surveydate': '2023-02-08T20:27:09+00:00', 'totaltime': 75, 'responses': {'605148537': '3982038472', '605148539': '3982038483', '605148542': '3982038499', '605148543': '3982038504', '605148544': '3982038508', '605148545': '3982038511', '605148546': '3982038515', '605148547': '3982038519', '605148548': '3982038523', '605148549': '3982038529', '605148550': '3982038534'}}

        try:
            person_dict = dict( person )
            ans_list = person_dict.get('responses',"")
            for q,a  in  ans_list.items() :
                ans = a
                if not a: ans= 'None'
                #replace all single quotes with two single quotes
                ans = ans.replace("\n"," ").replace("\r", " ")
                # ans = ans.replace("'","''").replace("\n"," ").replace("\r", " ")
                # sql_str = f"{stmt} {person_dict.get('surveyid')!r}, {person_dict.get('person')!r}, {q!r}, '{ans}' );\n"
                str = f"{person_dict.get('surveyid')!r},{person_dict.get('person')!r},{q!r},\"{ans}\"\n"
                response_str.append( str )
        except:
            print( f'##### ERROR:  {person}')

    return response_headers, response_str


def writecsv( func, headers, results ):
    schema_name = 'SURVEY_MONKEY'
    filename = f".\{schema_name}_{func}_OUTPUT.csv"

    # write_csv(fname,rows,raw=False,delim='\t',term='\n',prefix='',sortit=True,log=None,verify=False):
    with open( filename, 'w', newline = '') as file2write:
        file2write.write( f'{headers}\n')
        for row in results:
            file2write.write( row )
            
    print( f' --- Wrote {func} CSV file to: {filename}\n')


def main():
    # create a list of Survey objects
    filepath = Path( './' )
    surveys = [ Survey(**survey) for survey in all_surveys['data'] ]
    #print survey titles

    survey_sql = []
    survey_headers = "SURVEY_ID,SURVEY_NAME"
    # survey_sql.append( f'--SURVEYS available: {len(surveys)} --\n')

    for survey in surveys:
        # sql_str =  f"insert into SURVEY_MONKEY.SURVEYS values ( {survey.id!r}, '{survey.title}' );\n"
        str =  f'{survey.id!r},"{survey.title}"\n'
        survey_sql.append( str )

    detail1 = 'OHIO_BUREAU_OF_WORKERS_COMPENSATION_EMPLOYER_AUDIT_SURVEY_details.json'
    response1 = 'OHIO_BUREAU_OF_WORKERS_COMPENSATION_EMPLOYER_AUDIT_SURVEY_responses.json'
    print( f'--- Reading survey details from {detail1} ---' )
    detail_info = read_json_from_file( filepath/detail1 )

    print( f'--- Reading response information from {response1} ---' )
    response_info = read_json_from_file( filepath/response1 )

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

    writecsv( 'SURVEYS', survey_headers, survey_sql )
    writecsv( 'QUESTION', question_headers, question_sql )
    writecsv( 'ANSWERS', answer_headers, answer_sql )
    writecsv( 'PARTICIPANTS', participant_headers, participant_sql )
    writecsv( 'RESPONSES', response_headers, response_sql )
    

if __name__ == "__main__":
    main()
    