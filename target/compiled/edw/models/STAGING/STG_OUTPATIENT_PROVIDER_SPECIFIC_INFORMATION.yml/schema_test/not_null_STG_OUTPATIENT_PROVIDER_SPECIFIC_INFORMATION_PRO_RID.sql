
    
    



select count(*) as validation_errors
from STAGING.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION
where PRO_RID is null


