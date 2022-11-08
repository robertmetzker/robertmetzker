
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_ACCIDENT_DESC_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_ACCIDENT_DESCRIPTION
    where CLAIM_ACCIDENT_DESC_HKEY is not null
    group by CLAIM_ACCIDENT_DESC_HKEY
    having count(*) > 1

) validation_errors


