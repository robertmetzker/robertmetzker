
    
    



select count(*) as validation_errors
from (

    select
        CDES_ID

    from STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE
    where CDES_ID is not null
    group by CDES_ID
    having count(*) > 1

) validation_errors


