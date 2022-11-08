
    
    



select count(*) as validation_errors
from (

    select
        CLM_OTHR_DT_ID

    from STAGING.STG_CLAIM_OTHER_DATE
    where CLM_OTHR_DT_ID is not null
    group by CLM_OTHR_DT_ID
    having count(*) > 1

) validation_errors


