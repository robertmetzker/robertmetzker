
    
    



select count(*) as validation_errors
from (

    select
        DRG_HKEY

    from EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP
    where DRG_HKEY is not null
    group by DRG_HKEY
    having count(*) > 1

) validation_errors


