
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PREMIUM_CALCULATION_TYPE
where PREMIUM_CALCULATION_TYPE_HKEY is null


