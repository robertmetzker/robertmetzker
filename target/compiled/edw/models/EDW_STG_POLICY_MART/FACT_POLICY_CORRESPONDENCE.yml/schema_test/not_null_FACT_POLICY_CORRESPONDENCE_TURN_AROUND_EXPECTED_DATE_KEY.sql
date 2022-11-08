
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_CORRESPONDENCE
where TURN_AROUND_EXPECTED_DATE_KEY is null


