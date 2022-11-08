
    
    



select count(*) as validation_errors
from STAGING.DST_EOB
where UNIQUE_ID_KEY is null


