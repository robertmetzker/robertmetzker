
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_TYPE_STATUS_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_TYPE_STATUS
    where CLAIM_TYPE_STATUS_HKEY is not null
    group by CLAIM_TYPE_STATUS_HKEY
    having count(*) > 1

) validation_errors


