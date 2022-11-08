
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_CLAIM_TYPE
    where CLAIM_TYPE_HKEY is not null
    group by CLAIM_TYPE_HKEY
    having count(*) > 1

) validation_errors


