
    
    



select count(*) as validation_errors
from (

    select
        HEADER_INVOICE_HEADER_ID||'-'||INVOICE_NUMBER||'-'||LINE_SEQUENCE||'-'||LINE_VERSION_NUMBER||'-'||SEQUENCE_EXTENSION||'-'||LINE_DLM

    from STAGING.DST_INVOICE_PRESCRIPTION
    where HEADER_INVOICE_HEADER_ID||'-'||INVOICE_NUMBER||'-'||LINE_SEQUENCE||'-'||LINE_VERSION_NUMBER||'-'||SEQUENCE_EXTENSION||'-'||LINE_DLM is not null
    group by HEADER_INVOICE_HEADER_ID||'-'||INVOICE_NUMBER||'-'||LINE_SEQUENCE||'-'||LINE_VERSION_NUMBER||'-'||SEQUENCE_EXTENSION||'-'||LINE_DLM
    having count(*) > 1

) validation_errors


