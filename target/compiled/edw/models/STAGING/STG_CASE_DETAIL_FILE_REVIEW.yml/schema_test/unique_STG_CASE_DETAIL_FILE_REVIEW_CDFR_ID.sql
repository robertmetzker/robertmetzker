
    
    



select count(*) as validation_errors
from (

    select
        CDFR_ID

    from STAGING.STG_CASE_DETAIL_FILE_REVIEW
    where CDFR_ID is not null
    group by CDFR_ID
    having count(*) > 1

) validation_errors


