
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_CORRESPONDENCE
where DOCUMENT_STATE_DATE_KEY is null


