
    
    



select count(*) as validation_errors
from STAGING.DSV_DATE
where DATE_DIM_KEY is null


