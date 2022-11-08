
    
    



select count(*) as validation_errors
from (

    select
        CDL_ID

    from STAGING.STG_CASE_DETAIL_LEGAL
    where CDL_ID is not null
    group by CDL_ID
    having count(*) > 1

) validation_errors


