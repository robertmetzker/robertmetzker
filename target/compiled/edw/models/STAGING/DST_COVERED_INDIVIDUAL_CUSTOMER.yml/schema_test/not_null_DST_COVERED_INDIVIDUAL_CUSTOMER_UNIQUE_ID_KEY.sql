
    
    



select count(*) as validation_errors
from STAGING.DST_COVERED_INDIVIDUAL_CUSTOMER
where UNIQUE_ID_KEY is null


