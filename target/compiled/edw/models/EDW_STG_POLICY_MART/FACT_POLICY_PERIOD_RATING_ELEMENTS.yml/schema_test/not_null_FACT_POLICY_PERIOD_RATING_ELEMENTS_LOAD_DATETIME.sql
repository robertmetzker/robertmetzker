
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_PERIOD_RATING_ELEMENTS
where LOAD_DATETIME is null


