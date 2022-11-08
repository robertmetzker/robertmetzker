
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING
where INVOICE_LINE_ITEM_STATUS_HKEY is null


