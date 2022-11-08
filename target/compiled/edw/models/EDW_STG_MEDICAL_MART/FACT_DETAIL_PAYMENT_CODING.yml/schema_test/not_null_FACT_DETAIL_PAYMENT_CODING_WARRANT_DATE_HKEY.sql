
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_DETAIL_PAYMENT_CODING
where WARRANT_DATE_HKEY is null


