
    
    



select count(*) as validation_errors
from STAGING.DSV_ICD
where UNIQUE_ID_KEY is null


