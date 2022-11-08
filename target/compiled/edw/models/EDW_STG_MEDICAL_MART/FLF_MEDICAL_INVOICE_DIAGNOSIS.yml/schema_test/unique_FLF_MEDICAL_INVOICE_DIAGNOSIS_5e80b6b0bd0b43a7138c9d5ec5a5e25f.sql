
    
    



select count(*) as validation_errors
from (

    select
        MEDICAL_INVOICE_NUMBER||'-'||HEADER_DIAGNOSIS_HKEY||'-'||DIAGNOSIS_SEQUENCE_NUMBER

    from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_DIAGNOSIS
    where MEDICAL_INVOICE_NUMBER||'-'||HEADER_DIAGNOSIS_HKEY||'-'||DIAGNOSIS_SEQUENCE_NUMBER is not null
    group by MEDICAL_INVOICE_NUMBER||'-'||HEADER_DIAGNOSIS_HKEY||'-'||DIAGNOSIS_SEQUENCE_NUMBER
    having count(*) > 1

) validation_errors


