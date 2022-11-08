
    
    



select count(*) as validation_errors
from STAGING.STG_TDDIDCS
where ICD_CODE_STATUS_EFFECTIVE_DATE is null


