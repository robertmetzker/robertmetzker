
    
    



select count(*) as validation_errors
from (

    select
        REPRESENTATIVE_HKEY

    from EDW_STAGING_DIM.DIM_REPRESENTATIVE
    where REPRESENTATIVE_HKEY is not null
    group by REPRESENTATIVE_HKEY
    having count(*) > 1

) validation_errors


