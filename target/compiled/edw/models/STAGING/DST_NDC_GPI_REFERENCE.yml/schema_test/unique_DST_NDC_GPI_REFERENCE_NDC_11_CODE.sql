
    
    



select count(*) as validation_errors
from (

    select
        NDC_11_CODE

    from STAGING.DST_NDC_GPI_REFERENCE
    where NDC_11_CODE is not null
    group by NDC_11_CODE
    having count(*) > 1

) validation_errors


