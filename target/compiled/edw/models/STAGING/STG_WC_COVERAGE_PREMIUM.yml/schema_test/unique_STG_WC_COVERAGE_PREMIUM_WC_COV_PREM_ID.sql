
    
    



select count(*) as validation_errors
from (

    select
        WC_COV_PREM_ID

    from STAGING.STG_WC_COVERAGE_PREMIUM
    where WC_COV_PREM_ID is not null
    group by WC_COV_PREM_ID
    having count(*) > 1

) validation_errors


