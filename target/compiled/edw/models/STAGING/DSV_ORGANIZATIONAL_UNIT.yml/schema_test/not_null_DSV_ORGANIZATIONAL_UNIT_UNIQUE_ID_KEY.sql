
    
    



select count(*) as validation_errors
from STAGING.DSV_ORGANIZATIONAL_UNIT
where UNIQUE_ID_KEY is null


