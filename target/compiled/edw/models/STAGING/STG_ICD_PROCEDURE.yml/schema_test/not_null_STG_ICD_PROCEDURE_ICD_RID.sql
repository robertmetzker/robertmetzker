
    
    



select count(*) as validation_errors
from STAGING.STG_ICD_PROCEDURE
where ICD_RID is null


