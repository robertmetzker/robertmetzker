
    
    



select count(*) as validation_errors
from STAGING.DSV_RATING_ELEMENT
where UNIQUE_ID_KEY is null


