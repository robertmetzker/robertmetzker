
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_POLICY_PERIOD_RATING_ELEMENTS
where YEAR_CONTROL_ELEMENT_HKEY is null


