
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PBM_PRICING_METHOD
where UNIQUE_ID_KEY is null


