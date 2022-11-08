
    
    



select count(*) as validation_errors
from (

    select
        CLM_DISAB_MANG_ID

    from STAGING.STG_CLAIM_DISABILITY_MANAGEMENT
    where CLM_DISAB_MANG_ID is not null
    group by CLM_DISAB_MANG_ID
    having count(*) > 1

) validation_errors


