
    
    



select count(*) as validation_errors
from STAGING.DST_PREMIUM_CALCULATION_TYPE
where UNIQUE_ID_KEY is null


