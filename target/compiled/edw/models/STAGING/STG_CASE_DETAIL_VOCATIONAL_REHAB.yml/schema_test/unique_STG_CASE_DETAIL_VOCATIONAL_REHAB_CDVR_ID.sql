
    
    



select count(*) as validation_errors
from (

    select
        CDVR_ID

    from STAGING.STG_CASE_DETAIL_VOCATIONAL_REHAB
    where CDVR_ID is not null
    group by CDVR_ID
    having count(*) > 1

) validation_errors


