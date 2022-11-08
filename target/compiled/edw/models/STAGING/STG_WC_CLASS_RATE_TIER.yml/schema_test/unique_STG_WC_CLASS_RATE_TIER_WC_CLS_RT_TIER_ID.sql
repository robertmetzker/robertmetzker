
    
    



select count(*) as validation_errors
from (

    select
        WC_CLS_RT_TIER_ID

    from STAGING.STG_WC_CLASS_RATE_TIER
    where WC_CLS_RT_TIER_ID is not null
    group by WC_CLS_RT_TIER_ID
    having count(*) > 1

) validation_errors


