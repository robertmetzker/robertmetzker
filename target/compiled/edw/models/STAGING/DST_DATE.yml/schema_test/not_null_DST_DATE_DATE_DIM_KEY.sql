
    
    



select count(*) as validation_errors
from STAGING.DST_DATE
where DATE_DIM_KEY is null


