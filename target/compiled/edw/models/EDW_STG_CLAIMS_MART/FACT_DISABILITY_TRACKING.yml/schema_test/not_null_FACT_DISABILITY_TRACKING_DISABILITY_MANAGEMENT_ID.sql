
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_DISABILITY_TRACKING
where DISABILITY_MANAGEMENT_ID is null


