
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_BILLING
where LOAD_DATETIME is null


