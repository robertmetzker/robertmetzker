
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_WRITTEN_PREMIUM
where COVERED_INDIVIDUAL_CUSTOMER_HKEY is null


