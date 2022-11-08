
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ADDRESS_MAIL
where MAIL_ADRS_EFF_DATE is null


