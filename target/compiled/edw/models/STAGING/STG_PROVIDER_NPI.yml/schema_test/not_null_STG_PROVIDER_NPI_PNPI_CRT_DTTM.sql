
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_NPI
where PNPI_CRT_DTTM is null


