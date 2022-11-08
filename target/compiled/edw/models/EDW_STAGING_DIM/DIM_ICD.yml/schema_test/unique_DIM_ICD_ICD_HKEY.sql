
    
    



select count(*) as validation_errors
from (

    select
        ICD_HKEY

    from EDW_STAGING_DIM.DIM_ICD
    where ICD_HKEY is not null
    group by ICD_HKEY
    having count(*) > 1

) validation_errors


