
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_WRITTEN_PREMIUM
where WRITTEN_PREMIUM_ELEMENT_HKEY is null


