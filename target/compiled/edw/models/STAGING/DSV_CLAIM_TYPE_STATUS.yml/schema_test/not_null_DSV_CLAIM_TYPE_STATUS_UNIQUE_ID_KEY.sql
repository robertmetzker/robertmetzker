
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_TYPE_STATUS
where UNIQUE_ID_KEY is null


