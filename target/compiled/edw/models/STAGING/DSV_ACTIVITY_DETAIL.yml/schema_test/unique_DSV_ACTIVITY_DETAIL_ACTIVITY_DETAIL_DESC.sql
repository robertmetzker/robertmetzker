
    
    



select count(*) as validation_errors
from (

    select
        ACTIVITY_DETAIL_DESC

    from STAGING.DSV_ACTIVITY_DETAIL
    where ACTIVITY_DETAIL_DESC is not null
    group by ACTIVITY_DETAIL_DESC
    having count(*) > 1

) validation_errors


