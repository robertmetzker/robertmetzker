
    
    



select count(*) as validation_errors
from STAGING.DSV_ICD_ADMISSION_PRESENCE
where UNIQUE_ID_KEY is null


