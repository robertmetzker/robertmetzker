
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DATE
where DATE_KEY is null


