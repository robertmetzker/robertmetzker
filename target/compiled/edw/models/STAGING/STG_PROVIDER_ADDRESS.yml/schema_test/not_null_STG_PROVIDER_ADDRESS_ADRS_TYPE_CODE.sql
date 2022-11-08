
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_ADDRESS
where ADRS_TYPE_CODE is null


