
    
    



select count(*) as validation_errors
from STAGING.DSV_ICD
where ICD_CODE is null


