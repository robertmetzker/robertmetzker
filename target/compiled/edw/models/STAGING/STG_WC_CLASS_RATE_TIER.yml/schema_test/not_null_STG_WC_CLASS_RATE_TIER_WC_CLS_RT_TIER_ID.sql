
    
    



select count(*) as validation_errors
from STAGING.STG_WC_CLASS_RATE_TIER
where WC_CLS_RT_TIER_ID is null


