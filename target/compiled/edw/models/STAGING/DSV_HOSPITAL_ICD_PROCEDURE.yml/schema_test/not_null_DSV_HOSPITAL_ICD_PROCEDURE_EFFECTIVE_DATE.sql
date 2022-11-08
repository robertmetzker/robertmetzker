
    
    



select count(*) as validation_errors
from STAGING.DSV_HOSPITAL_ICD_PROCEDURE
where EFFECTIVE_DATE is null


