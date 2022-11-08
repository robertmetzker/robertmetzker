
    
    



select count(*) as validation_errors
from (

    select
        CASE_SERV_ID

    from STAGING.STG_CASE_SERVICE
    where CASE_SERV_ID is not null
    group by CASE_SERV_ID
    having count(*) > 1

) validation_errors


