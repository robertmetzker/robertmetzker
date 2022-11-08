
    
    



select count(*) as validation_errors
from STAGING.DST_DIAGNOSIS_RELATED_GROUP
where UNIQUE_ID_KEY is null


