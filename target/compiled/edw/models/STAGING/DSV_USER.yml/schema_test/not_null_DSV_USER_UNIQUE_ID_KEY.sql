
    
    



select count(*) as validation_errors
from STAGING.DSV_USER
where UNIQUE_ID_KEY is null


