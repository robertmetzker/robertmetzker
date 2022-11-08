
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_COVERAGE
where CLM_COV_ID is null


