
    
    



select count(*) as validation_errors
from (

    select
        PLCY_SUM_DTL_ID

    from STAGING.STG_POLICY_SUMMARY_DETAIL
    where PLCY_SUM_DTL_ID is not null
    group by PLCY_SUM_DTL_ID
    having count(*) > 1

) validation_errors


