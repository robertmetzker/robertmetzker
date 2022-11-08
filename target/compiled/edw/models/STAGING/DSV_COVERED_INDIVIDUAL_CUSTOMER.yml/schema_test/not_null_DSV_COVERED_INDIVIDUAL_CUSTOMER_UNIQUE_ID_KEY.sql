
    
    



select count(*) as validation_errors
from STAGING.DSV_COVERED_INDIVIDUAL_CUSTOMER
where UNIQUE_ID_KEY is null


