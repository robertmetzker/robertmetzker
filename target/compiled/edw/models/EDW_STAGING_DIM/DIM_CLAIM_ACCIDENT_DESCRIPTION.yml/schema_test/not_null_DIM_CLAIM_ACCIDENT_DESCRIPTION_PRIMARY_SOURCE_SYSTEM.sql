
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CLAIM_ACCIDENT_DESCRIPTION
where PRIMARY_SOURCE_SYSTEM is null


