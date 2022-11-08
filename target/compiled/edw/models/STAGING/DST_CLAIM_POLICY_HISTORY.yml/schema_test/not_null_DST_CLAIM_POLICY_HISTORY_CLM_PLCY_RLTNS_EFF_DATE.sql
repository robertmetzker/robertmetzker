
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_POLICY_HISTORY
where CLM_PLCY_RLTNS_EFF_DATE is null


