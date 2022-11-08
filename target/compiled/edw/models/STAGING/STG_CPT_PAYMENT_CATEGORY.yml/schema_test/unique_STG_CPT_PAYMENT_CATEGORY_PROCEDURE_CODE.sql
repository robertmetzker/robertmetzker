
    
    



select count(*) as validation_errors
from (

    select
        PROCEDURE_CODE

    from STAGING.STG_CPT_PAYMENT_CATEGORY
    where PROCEDURE_CODE is not null
    group by PROCEDURE_CODE
    having count(*) > 1

) validation_errors


