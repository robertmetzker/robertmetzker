
    
    



select count(*) as validation_errors
from STAGING.DST_ORGANIZATIONAL_UNIT
where UNIQUE_ID_KEY is null


