
    
    



select count(*) as validation_errors
from (

    select
        CASE_ISS_ID

    from STAGING.STG_CASE_ISSUE
    where CASE_ISS_ID is not null
    group by CASE_ISS_ID
    having count(*) > 1

) validation_errors


