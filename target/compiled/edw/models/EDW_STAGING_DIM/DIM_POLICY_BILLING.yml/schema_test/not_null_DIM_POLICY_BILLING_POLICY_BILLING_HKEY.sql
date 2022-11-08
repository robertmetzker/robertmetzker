
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_POLICY_BILLING
where POLICY_BILLING_HKEY is null


