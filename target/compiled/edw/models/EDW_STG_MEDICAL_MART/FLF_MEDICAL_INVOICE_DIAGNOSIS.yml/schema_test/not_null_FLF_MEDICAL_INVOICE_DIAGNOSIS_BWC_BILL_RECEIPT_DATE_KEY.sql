
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_DIAGNOSIS
where BWC_BILL_RECEIPT_DATE_KEY is null


