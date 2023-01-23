
    
    



select count(*) as validation_errors
from (

    select
        HEADER_PROCEDURE_HKEY||MEDICAL_INVOICE_NUMBER||PROCEDURE_SEQUENCE_NUMBER

    from EDW_STG_MEDICAL_MART.FLF_MEDICAL_INVOICE_PROCEDURES
    where HEADER_PROCEDURE_HKEY||MEDICAL_INVOICE_NUMBER||PROCEDURE_SEQUENCE_NUMBER is not null
    group by HEADER_PROCEDURE_HKEY||MEDICAL_INVOICE_NUMBER||PROCEDURE_SEQUENCE_NUMBER
    having count(*) > 1

) validation_errors

