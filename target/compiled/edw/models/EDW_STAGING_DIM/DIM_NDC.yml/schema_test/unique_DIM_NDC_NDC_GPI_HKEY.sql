
    
    



select count(*) as validation_errors
from (

    select
        NDC_GPI_HKEY

    from EDW_STAGING_DIM.DIM_NDC
    where NDC_GPI_HKEY is not null
    group by NDC_GPI_HKEY
    having count(*) > 1

) validation_errors


