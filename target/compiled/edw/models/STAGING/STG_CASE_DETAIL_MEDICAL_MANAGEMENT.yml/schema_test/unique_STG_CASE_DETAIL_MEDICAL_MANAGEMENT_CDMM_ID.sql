
    
    



select count(*) as validation_errors
from (

    select
        CDMM_ID

    from STAGING.STG_CASE_DETAIL_MEDICAL_MANAGEMENT
    where CDMM_ID is not null
    group by CDMM_ID
    having count(*) > 1

) validation_errors


