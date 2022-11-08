
    
    



select count(*) as validation_errors
from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_INPATIENT_BILLING
where MEDICAL_INVOICE_HEADER_VERSION_NUMBER is null


