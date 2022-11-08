
    
    



select count(*) as validation_errors
from (

    select
        PROCEDURE_CODE

    from STAGING.STG_CPT_VR_FEE_SCHEDULE
    where PROCEDURE_CODE is not null
    group by PROCEDURE_CODE
    having count(*) > 1

) validation_errors


