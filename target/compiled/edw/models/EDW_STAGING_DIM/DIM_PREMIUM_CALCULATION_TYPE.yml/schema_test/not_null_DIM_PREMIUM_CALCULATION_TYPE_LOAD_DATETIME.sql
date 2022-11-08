
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PREMIUM_CALCULATION_TYPE
where LOAD_DATETIME is null


