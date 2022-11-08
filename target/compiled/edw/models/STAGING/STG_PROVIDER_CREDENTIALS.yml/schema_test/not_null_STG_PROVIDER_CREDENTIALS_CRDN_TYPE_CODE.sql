
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_CREDENTIALS
where CRDN_TYPE_CODE is null


