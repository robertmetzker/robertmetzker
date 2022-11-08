
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_BWC_CALC_DRG_OUTPUT
where LOAD_DATETIME is null


