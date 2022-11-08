
    
    



select count(*) as validation_errors
from STAGING.DSV_HOSPITAL_ICD_PROCEDURE
where ICD_PROCEDURE_CODE is null


