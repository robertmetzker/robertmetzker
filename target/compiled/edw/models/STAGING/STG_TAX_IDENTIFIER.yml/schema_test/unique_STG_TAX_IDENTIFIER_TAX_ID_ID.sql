
    
    



select count(*) as validation_errors
from (

    select
        TAX_ID_ID

    from STAGING.STG_TAX_IDENTIFIER
    where TAX_ID_ID is not null
    group by TAX_ID_ID
    having count(*) > 1

) validation_errors


