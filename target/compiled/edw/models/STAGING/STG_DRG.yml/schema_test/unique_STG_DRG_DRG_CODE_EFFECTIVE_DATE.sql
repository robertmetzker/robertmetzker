
    
    



select count(*) as validation_errors
from (

    select
        DRG_CODE||EFFECTIVE_DATE

    from STAGING.STG_DRG
    where DRG_CODE||EFFECTIVE_DATE is not null
    group by DRG_CODE||EFFECTIVE_DATE
    having count(*) > 1

) validation_errors


