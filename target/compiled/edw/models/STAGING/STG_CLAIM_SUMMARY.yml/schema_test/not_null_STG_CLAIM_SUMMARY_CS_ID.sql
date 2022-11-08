
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_SUMMARY
where CS_ID is null


