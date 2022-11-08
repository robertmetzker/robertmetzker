
    
    



select count(*) as validation_errors
from STAGING.DSV_COVERED_INDIVIDUAL_CUSTOMER
where COVERED_INDIVIDUAL_CUSTOMER_NUMBER is null


