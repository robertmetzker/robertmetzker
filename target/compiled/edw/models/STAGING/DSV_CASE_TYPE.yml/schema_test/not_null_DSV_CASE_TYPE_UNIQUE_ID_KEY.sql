
    
    



select count(*) as validation_errors
from STAGING.DSV_CASE_TYPE
where UNIQUE_ID_KEY is null


