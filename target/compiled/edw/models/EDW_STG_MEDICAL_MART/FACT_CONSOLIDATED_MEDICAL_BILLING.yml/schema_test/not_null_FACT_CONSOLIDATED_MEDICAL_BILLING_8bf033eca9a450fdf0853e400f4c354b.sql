
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_CONSOLIDATED_MEDICAL_BILLING
where MEDICAL_INVOICE_LINE_VERSION_NUMBER is null

