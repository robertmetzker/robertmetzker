
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_AVERAGE_WAGE
where CLM_AVG_WG_ID is null


