import requests, json
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




# pass filenames containing the survey details and responses

def read_details_from_file(filename):
    with open(filename) as f:
        details = json.load(f)
    return details


def read_responses_from_file(filename):
    with open(filename) as f:
        responses = json.load(f)
    return responses


def show_questions(detail_info):
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


def add_question_answers_to_df(df: dict, detail_info: dict):
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
        else:
            ans[answer["id"]] = 'Free Form Field'

        # Add the ans dictionary to  dictionary within df[question["id"]]["answers"]
        # df[question["id"]]["answers"] = ans
        survey_info.update(ans)

        # print(survey_info )
    return survey_info 


def show_responses(response_data, parsed_questions):
    # print( response_data )
    print('Recipent: ', response_data[0]["recipient_id"]) 
    print('FName: ', response_data[0]["first_name"]) 
    print('LName: ', response_data[0]["last_name"]) 
    print('Email: ', response_data[0]["metadata"]["contact"]["email"]["value"]) 

    print('SurveyID: ', response_data[0]["survey_id"]) 
    # print('Answers: ', response_data[0]["pages"][0]["questions"]) 

    for answer in response_data[0]["pages"][0]["questions"]:
        qid = answer["id"]
        aid = answer["answers"][0]["choice_id"]
        print( f'QUESTION: {qid},  {parsed_questions[qid]}' )
        print( f'  ANSWER: {aid}, {parsed_questions[aid]}' )


def main():
    # create a list of Survey objects
    filepath = Path( '/Users/rmetzk/Desktop/python/robertmetzker/survey-monkey-api-wrapper-master/' )
    surveys = [ Survey(**survey) for survey in all_surveys['data'] ]
    #print survey titles
    print(f'--SURVEYS available: {len(surveys)} --')
    for survey in surveys:
        print(survey.id, ':', survey.title)
    print('-'*40)

    detail1 = 'OHIO_BUREAU_OF_WORKERS_COMPENSATION_EMPLOYER_AUDIT_SURVEY_details.json'
    print( f'--- Reading survey details from {detail1} ---' )
    detail_info = read_details_from_file( filepath/detail1 )

    survey_info = {}

    survey_info['id'] = detail_info['id']
    survey_info['title'] = detail_info['title']
    survey_info['question_count'] = detail_info['question_count']
    survey_info['date_created'] = detail_info['date_created']

    # append each question as a dictionary to the survey_info dictionary using the ID as the key, and the question text as the value
    print( survey_info )
    print('-'*40)

    show_questions( detail_info )
    parsed_questions = add_question_answers_to_df(survey_info, detail_info)
    print( parsed_questions )

    print(f'\n\n##- Parsing example response data --')
    show_responses( response_data, parsed_questions )

if __name__ == "__main__":
    main()
    
