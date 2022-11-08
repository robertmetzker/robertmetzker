
    
    



select count(*) as validation_errors
from (

    select
        CASE_PTCP_ID

    from STAGING.STG_CASE_PARTICIPATION
    where CASE_PTCP_ID is not null
    group by CASE_PTCP_ID
    having count(*) > 1

) validation_errors


