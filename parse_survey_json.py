import requests, json
import pandas as pd
from dataclasses import dataclass, field

all_surveys = {"data": [
    {"id": "300747984", "title": "Ohio Bureau of Workers' Compensation Employer Audit Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/300747984"}, 
    {"id": "303452492", "title": "Ohio BWC Employer Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/303452492"}, 
    {"id": "112758772", "title": "IW Pulse Survey\u00a0 - 30 Day", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/112758772"}, 
    {"id": "301195553", "title": "IW Pulse Survey - 90 Day", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/301195553"}, 
    {"id": "401765993", "title": "IW Pulse Survey\u00a0 - Medical Only", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/401765993"}, 
    {"id": "312123669", "title": "Ohio BWC\u00a0First\u00a0Report of Injury (FROI) Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/312123669"}, 
    {"id": "318130682", "title": "Ohio BWC MCO Customer Satisfaction Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/318130682"}, 
    {"id": "316723735", "title": "January 2022\u00a0Workshop Survey", "nickname": "", "href": "https://api.surveymonkey.com/v3/surveys/316723735"}, 
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




def main():
    # create a list of Survey objects
    surveys = [ Survey(**survey) for survey in all_surveys['data'] ]

    # # print the survey titles
    # for survey in surveys:
    #     print(survey.title)
    detail1 = 'OHIO_BUREAU_OF_WORKERS_COMPENSATION_EMPLOYER_AUDIT_SURVEY_details.json'
    print( f'--- Reading survey details from {detail1} ---' )
    detail_info = read_details_from_file( detail1 )

        # print( detail_info )



    results1 = 'OHIO_BUREAU_OF_WORKERS_COMPENSATION_EMPLOYER_AUDIT_SURVEY_responses.json'
    print( f'--- Reading survey responses from {results1} ---' )
    result_info = read_responses_from_file( results1 )
    # print( result_info )


if __name__ == "__main__":
    main()
    