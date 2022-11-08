
    
    



select count(*) as validation_errors
from STAGING.STG_CORESUITE_CONVERSION_CLAIM_STATUS
where CLAIM_STATUS_REASON_CODE is null


