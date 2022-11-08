
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_NDC_PRICING
where PRIMARY_SOURCE_SYSTEM is null


