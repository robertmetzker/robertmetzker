
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_PERIOD_PREMIUM_DRV
where PLCY_PRD_PREM_DRV_ID is null

