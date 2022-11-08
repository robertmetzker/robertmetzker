
    
    



select count(*) as validation_errors
from STAGING.DST_ICD
where UNIQUE_ID_KEY is null


