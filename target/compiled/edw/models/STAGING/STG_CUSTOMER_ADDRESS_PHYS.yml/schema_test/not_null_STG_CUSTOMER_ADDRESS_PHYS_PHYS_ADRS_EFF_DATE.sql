
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ADDRESS_PHYS
where PHYS_ADRS_EFF_DATE is null


