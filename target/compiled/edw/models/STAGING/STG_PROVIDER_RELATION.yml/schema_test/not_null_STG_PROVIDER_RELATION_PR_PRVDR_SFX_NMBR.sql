
    
    



select count(*) as validation_errors
from STAGING.STG_PROVIDER_RELATION
where PR_PRVDR_SFX_NMBR is null


