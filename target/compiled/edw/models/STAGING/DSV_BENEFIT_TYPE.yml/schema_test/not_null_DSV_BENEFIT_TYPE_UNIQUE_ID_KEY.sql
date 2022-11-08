
    
    



select count(*) as validation_errors
from STAGING.DSV_BENEFIT_TYPE
where UNIQUE_ID_KEY is null


