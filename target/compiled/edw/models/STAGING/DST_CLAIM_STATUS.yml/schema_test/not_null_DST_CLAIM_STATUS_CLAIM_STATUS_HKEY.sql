
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_STATUS
where CLAIM_STATUS_HKEY is null


