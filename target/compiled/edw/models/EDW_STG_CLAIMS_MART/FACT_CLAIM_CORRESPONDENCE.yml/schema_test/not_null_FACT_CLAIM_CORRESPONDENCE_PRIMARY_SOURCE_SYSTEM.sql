
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_CORRESPONDENCE
where PRIMARY_SOURCE_SYSTEM is null


