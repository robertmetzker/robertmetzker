
    
    



select count(*) as validation_errors
from STAGING.DSV_REVENUE_CODE
where UNIQUE_ID_KEY is null


