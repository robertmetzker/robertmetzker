
    
    



select count(*) as validation_errors
from STAGING.DSV_CPT_CODE
where PROCEDURE_CODE_EFFECTIVE_DATE is null


