
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_ICD_STATUS_DETAIL_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_ICD_STATUS_DETAIL
    where CLAIM_ICD_STATUS_DETAIL_HKEY is not null
    group by CLAIM_ICD_STATUS_DETAIL_HKEY
    having count(*) > 1

) validation_errors


