
    
    



select count(*) as validation_errors
from (

    select
        PROCEDURE_CODE

    from STAGING.DSV_CPT_CODE
    where PROCEDURE_CODE is not null
    group by PROCEDURE_CODE
    having count(*) > 1

) validation_errors


