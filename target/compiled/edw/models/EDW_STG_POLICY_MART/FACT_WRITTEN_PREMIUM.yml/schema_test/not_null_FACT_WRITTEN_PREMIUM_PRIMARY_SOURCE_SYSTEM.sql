
    
    



select count(*) as validation_errors
from EDW_STG_POLICY_MART.FACT_WRITTEN_PREMIUM
where PRIMARY_SOURCE_SYSTEM is null

