
    
    



select count(*) as validation_errors
from (

    select
        PREMIUM_CALCULATION_TYPE_DESC

    from STAGING.STG_PREMIUM_CALCULATION_TYPE
    where PREMIUM_CALCULATION_TYPE_DESC is not null
    group by PREMIUM_CALCULATION_TYPE_DESC
    having count(*) > 1

) validation_errors


