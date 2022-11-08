
    
    



select count(*) as validation_errors
from (

    select
        CLM_WG_SRC_DTL_ID

    from STAGING.STG_CLAIM_WAGE_SOURCE_DETAIL
    where CLM_WG_SRC_DTL_ID is not null
    group by CLM_WG_SRC_DTL_ID
    having count(*) > 1

) validation_errors


