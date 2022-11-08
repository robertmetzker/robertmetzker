
    
    



select count(*) as validation_errors
from (

    select
        CASE_PRFL_ID

    from STAGING.STG_CASE_PROFILE
    where CASE_PRFL_ID is not null
    group by CASE_PRFL_ID
    having count(*) > 1

) validation_errors


