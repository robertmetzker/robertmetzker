
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_ALIAS_NUMBER
where CLM_ALIAS_NO_ID is null


