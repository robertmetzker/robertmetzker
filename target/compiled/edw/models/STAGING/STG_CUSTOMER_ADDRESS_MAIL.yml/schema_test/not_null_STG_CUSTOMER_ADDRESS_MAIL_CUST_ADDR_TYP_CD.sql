
    
    



select count(*) as validation_errors
from STAGING.STG_CUSTOMER_ADDRESS_MAIL
where CUST_ADDR_TYP_CD is null


