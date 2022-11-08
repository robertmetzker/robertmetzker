
    
    



select count(*) as validation_errors
from STAGING.DST_INDIVIDUAL_COVERAGE_TYPE
where UNIQUE_ID_KEY is null


