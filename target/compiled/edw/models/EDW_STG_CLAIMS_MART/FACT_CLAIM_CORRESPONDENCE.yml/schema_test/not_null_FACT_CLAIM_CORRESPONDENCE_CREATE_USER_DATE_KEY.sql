
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_CORRESPONDENCE
where CREATE_USER_DATE_KEY is null


