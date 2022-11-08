
    
    



select count(*) as validation_errors
from STAGING.DST_COVERED_INDIVIDUAL_CUSTOMER
where CUST_NO is null


