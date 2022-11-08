
    
    



select count(*) as validation_errors
from (

    select
        CAWWSD_ID

    from STAGING.STG_CLAIM_AVERAGE_WAGE_SOURCE_DETAIL
    where CAWWSD_ID is not null
    group by CAWWSD_ID
    having count(*) > 1

) validation_errors


