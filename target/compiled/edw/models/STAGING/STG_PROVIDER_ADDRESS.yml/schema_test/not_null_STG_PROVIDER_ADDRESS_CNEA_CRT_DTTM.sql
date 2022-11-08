
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_ADDRESS
where CNEA_CRT_DTTM is null


