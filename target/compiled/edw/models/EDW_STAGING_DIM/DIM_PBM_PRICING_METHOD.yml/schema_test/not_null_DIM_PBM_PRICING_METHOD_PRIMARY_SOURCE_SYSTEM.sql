
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PBM_PRICING_METHOD
where PRIMARY_SOURCE_SYSTEM is null


