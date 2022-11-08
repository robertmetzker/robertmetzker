
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_CORRESPONDENCE
where TURN_AROUND_RETURNED_DATE_KEY is null


