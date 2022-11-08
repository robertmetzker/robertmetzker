
    
    



select count(*) as validation_errors
from (

    select
        MEDICAL_INVOICE_NUMBER||'-'||MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER||'-'||MEDICAL_INVOICE_LINE_EXTENSION_NUMBER||'-'||MEDICAL_INVOICE_LINE_VERSION_NUMBER

    from EDW_STG_MEDICAL_MART.FACT_HOSPITAL_OUTPATIENT_BILLING
    where MEDICAL_INVOICE_NUMBER||'-'||MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER||'-'||MEDICAL_INVOICE_LINE_EXTENSION_NUMBER||'-'||MEDICAL_INVOICE_LINE_VERSION_NUMBER is not null
    group by MEDICAL_INVOICE_NUMBER||'-'||MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER||'-'||MEDICAL_INVOICE_LINE_EXTENSION_NUMBER||'-'||MEDICAL_INVOICE_LINE_VERSION_NUMBER
    having count(*) > 1

) validation_errors


