
    
    



select count(*) as validation_errors
from STAGING.DST_INDUSTRY_GROUP
where UNIQUE_ID_KEY is null


