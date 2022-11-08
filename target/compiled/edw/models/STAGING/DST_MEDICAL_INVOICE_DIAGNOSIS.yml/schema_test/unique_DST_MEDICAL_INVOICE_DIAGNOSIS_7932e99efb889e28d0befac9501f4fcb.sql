
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_NUMBER||'-'||SEQUENCE_NUMBER||'-'||DIAGNOSIS_CODE

    from STAGING.DST_MEDICAL_INVOICE_DIAGNOSIS
    where INVOICE_NUMBER||'-'||SEQUENCE_NUMBER||'-'||DIAGNOSIS_CODE is not null
    group by INVOICE_NUMBER||'-'||SEQUENCE_NUMBER||'-'||DIAGNOSIS_CODE
    having count(*) > 1

) validation_errors


