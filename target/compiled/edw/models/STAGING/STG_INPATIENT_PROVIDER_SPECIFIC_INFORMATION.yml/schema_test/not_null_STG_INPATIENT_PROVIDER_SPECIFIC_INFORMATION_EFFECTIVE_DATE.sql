
    
    



select count(*) as validation_errors
from STAGING.STG_INPATIENT_PROVIDER_SPECIFIC_INFORMATION
where EFFECTIVE_DATE is null


