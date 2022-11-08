
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_ADDRESS
where ADRS_TYPE_LCT_NMBR is null


