
    
    



select count(*) as validation_errors
from (

    select
        CPT_HKEY

    from EDW_STAGING_DIM.DIM_CPT
    where CPT_HKEY is not null
    group by CPT_HKEY
    having count(*) > 1

) validation_errors


