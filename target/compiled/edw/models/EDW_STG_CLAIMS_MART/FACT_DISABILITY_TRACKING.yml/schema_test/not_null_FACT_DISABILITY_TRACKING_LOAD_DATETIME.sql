
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_DISABILITY_TRACKING
where LOAD_DATETIME is null

