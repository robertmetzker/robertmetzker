
    
    



select count(*) as validation_errors
from (

    select
        EMPLOYER_HKEY

    from EDW_STAGING_DIM.DIM_EMPLOYER
    where EMPLOYER_HKEY is not null
    group by EMPLOYER_HKEY
    having count(*) > 1

) validation_errors


