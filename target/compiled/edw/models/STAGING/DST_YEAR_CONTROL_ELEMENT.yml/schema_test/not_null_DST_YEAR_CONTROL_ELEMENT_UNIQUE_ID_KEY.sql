
    
    



select count(*) as validation_errors
from STAGING.DST_YEAR_CONTROL_ELEMENT
where UNIQUE_ID_KEY is null


