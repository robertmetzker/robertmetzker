
    
    



select count(*) as validation_errors
from STAGING.DST_HOSPITAL_ICD_PROCEDURE
where EFFECTIVE_DATE is null


