
    
    



select count(*) as validation_errors
from (

    select
        CLM_ICD_STS_ID

    from STAGING.STG_CLAIM_ICD_STATUS
    where CLM_ICD_STS_ID is not null
    group by CLM_ICD_STS_ID
    having count(*) > 1

) validation_errors


