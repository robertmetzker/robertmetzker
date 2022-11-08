
    
    



select count(*) as validation_errors
from STAGING.DSV_DIAGNOSIS_RELATED_GROUP
where UNIQUE_ID_KEY is null


