
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_CORRESPONDENCE
where DOCUMENT_PENDING_DATE_KEY is null


