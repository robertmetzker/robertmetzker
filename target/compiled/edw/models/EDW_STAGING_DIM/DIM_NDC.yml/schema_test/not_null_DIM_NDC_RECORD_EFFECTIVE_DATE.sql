
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_NDC
where RECORD_EFFECTIVE_DATE is null


