
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_WRITTEN_PREMIUM
where COVERAGE_EFFECTIVE_DATE_KEY is null


