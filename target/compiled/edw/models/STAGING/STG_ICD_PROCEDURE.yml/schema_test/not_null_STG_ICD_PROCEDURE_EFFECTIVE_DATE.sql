
    
    



select count(*) as validation_errors
from STAGING.STG_ICD_PROCEDURE
where EFFECTIVE_DATE is null


