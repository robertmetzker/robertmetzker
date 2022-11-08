
    
    



select count(*) as validation_errors
from STAGING.DSV_HOSPITAL_ICD_PROCEDURE
where ICD_CODE_VERSION_NUMBER is null


