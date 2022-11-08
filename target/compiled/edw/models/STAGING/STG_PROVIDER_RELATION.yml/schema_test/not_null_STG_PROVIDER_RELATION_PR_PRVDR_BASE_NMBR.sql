
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_RELATION
where PR_PRVDR_BASE_NMBR is null


