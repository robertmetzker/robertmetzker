
    
    



select count(*) as validation_errors
from (

    select
        CASE_ID

    from STAGING.STG_CASES
    where CASE_ID is not null
    group by CASE_ID
    having count(*) > 1

) validation_errors


