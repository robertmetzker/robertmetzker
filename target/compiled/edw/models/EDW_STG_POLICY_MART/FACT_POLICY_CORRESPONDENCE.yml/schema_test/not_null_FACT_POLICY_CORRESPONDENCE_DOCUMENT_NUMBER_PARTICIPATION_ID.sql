
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_CORRESPONDENCE
where DOCUMENT_NUMBER || PARTICIPATION_ID is null


