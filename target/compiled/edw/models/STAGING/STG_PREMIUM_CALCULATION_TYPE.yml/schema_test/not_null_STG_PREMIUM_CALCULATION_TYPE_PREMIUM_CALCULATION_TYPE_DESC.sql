
    
    



select count(*) as validation_errors
from STAGING.STG_PREMIUM_CALCULATION_TYPE
where PREMIUM_CALCULATION_TYPE_DESC is null


