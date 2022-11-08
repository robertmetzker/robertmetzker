
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_NDC_PRICING
where NDC_GPI_HKEY is null


