
    
    



select count(*) as validation_errors
from (

    select
        ORG_UNT_ID

    from STAGING.DSV_ORGANIZATIONAL_UNIT
    where ORG_UNT_ID is not null
    group by ORG_UNT_ID
    having count(*) > 1

) validation_errors


