
    
    



select count(*) as validation_errors
from (

    select
        GROUPER_HKEY

    from EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER
    where GROUPER_HKEY is not null
    group by GROUPER_HKEY
    having count(*) > 1

) validation_errors


