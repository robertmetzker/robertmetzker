
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_ICD_DESC

    from EDW_STAGING_DIM.DIM_CLAIM_ICD_DESC
    where CLAIM_ICD_DESC is not null
    group by CLAIM_ICD_DESC
    having count(*) > 1

) validation_errors


