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

            if "other" in question["answers"]:
                try:
                    ans_id = question["answers"]["other"]["id"]
                    ans[ans_id] = question["answers"]["other"]["text"]
                except:
                    pass

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

if __name__ == "__main__":
    main()
    