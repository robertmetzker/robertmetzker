
    
    



select count(*) as validation_errors
from STAGING.DST_PREMIUM_CALCULATION_TYPE
where CURRENT_PREMIUM_CALCULATION_IND is null


