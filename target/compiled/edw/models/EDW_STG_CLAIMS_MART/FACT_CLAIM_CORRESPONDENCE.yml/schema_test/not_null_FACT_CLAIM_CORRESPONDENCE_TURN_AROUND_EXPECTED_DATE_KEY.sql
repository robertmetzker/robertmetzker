
    
    



select count(*) as validation_errors
from EDW_STG_CLAIMS_MART.FACT_CLAIM_CORRESPONDENCE
where TURN_AROUND_EXPECTED_DATE_KEY is null


