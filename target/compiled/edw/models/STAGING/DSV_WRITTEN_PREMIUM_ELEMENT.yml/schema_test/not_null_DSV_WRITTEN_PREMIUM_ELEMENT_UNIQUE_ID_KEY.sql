
    
    



select count(*) as validation_errors
from STAGING.DSV_WRITTEN_PREMIUM_ELEMENT
where UNIQUE_ID_KEY is null


