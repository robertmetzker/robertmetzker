
    
    



select count(*) as validation_errors
from STAGING.STG_CORESUITE_CONVERSION_CLAIM_STATUS
where CLAIM_STATE_CODE is null


