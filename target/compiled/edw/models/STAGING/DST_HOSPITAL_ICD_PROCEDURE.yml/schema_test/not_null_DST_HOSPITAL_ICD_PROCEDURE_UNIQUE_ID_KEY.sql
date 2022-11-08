
    
    



select count(*) as validation_errors
from STAGING.DST_HOSPITAL_ICD_PROCEDURE
where UNIQUE_ID_KEY is null


