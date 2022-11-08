
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_NDC_PRICING
where LOAD_DATETIME is null


