
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_BWC_CALC_DRG_OUTPUT
where BWC_CALC_DRG_OUTPUT_HKEY is null


