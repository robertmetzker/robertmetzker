
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_ICD_DESC_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_ICD_DESC
    where CLAIM_ICD_DESC_HKEY is not null
    group by CLAIM_ICD_DESC_HKEY
    having count(*) > 1

) validation_errors


