
    
    



select count(*) as validation_errors
from (

    select
        CLM_NO

    from STAGING.DST_CLAIM_INVESTIGATION
    where CLM_NO is not null
    group by CLM_NO
    having count(*) > 1

) validation_errors


