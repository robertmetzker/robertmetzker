
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_DISABILITY_TRACKING
where PRIMARY_SOURCE_SYSTEM is null


