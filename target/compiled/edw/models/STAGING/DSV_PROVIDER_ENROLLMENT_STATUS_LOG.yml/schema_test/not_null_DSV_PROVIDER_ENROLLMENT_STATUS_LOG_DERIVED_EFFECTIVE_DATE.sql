
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER_ENROLLMENT_STATUS_LOG
where DERIVED_EFFECTIVE_DATE is null


