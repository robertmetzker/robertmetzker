
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_NUMBER||PROCEDURE_CODE||PRO_SEQUENCE_NUMBER

    from STAGING.DST_MEDICAL_INVOICE_PROCEDURE
    where INVOICE_NUMBER||PROCEDURE_CODE||PRO_SEQUENCE_NUMBER is not null
    group by INVOICE_NUMBER||PROCEDURE_CODE||PRO_SEQUENCE_NUMBER
    having count(*) > 1

) validation_errors


