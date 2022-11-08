
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PROFESSIONAL_ASC_BILLING
where MEDICAL_INVOICE_LINE_EXTENSION_NUMBER is null


