
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_STATUS
where CLAIM_STATUS_HKEY is null


