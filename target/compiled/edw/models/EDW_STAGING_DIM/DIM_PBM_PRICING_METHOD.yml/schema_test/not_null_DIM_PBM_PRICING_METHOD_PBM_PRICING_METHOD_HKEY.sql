
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PBM_PRICING_METHOD
where PBM_PRICING_METHOD_HKEY is null


