
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM
where CLM_OCCR_DATE is null


