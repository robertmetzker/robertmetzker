
    
    



select count(*) as validation_errors
from STAGING.DST_PREMIUM_CALCULATION_TYPE
where PREMIUM_CALCULATION_TYPE_DESC is null


