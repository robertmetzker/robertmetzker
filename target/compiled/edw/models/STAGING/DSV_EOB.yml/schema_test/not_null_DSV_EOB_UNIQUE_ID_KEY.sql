
    
    



select count(*) as validation_errors
from STAGING.DSV_EOB
where UNIQUE_ID_KEY is null


