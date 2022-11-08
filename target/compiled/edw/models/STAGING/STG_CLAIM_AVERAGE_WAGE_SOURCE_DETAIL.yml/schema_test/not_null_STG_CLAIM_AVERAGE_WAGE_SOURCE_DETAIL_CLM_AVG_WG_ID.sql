
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_AVERAGE_WAGE_SOURCE_DETAIL
where CLM_AVG_WG_ID is null


