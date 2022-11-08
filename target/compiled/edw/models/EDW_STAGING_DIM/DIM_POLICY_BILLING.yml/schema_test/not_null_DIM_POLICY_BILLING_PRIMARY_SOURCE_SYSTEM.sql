
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_BILLING
where PRIMARY_SOURCE_SYSTEM is null


