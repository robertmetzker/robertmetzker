
    
    



select count(*) as validation_errors
from STAGING.DST_INVOICE_PRESCRIPTION
where HEADER_INVOICE_HEADER_ID||'-'||INVOICE_NUMBER||'-'||LINE_SEQUENCE||'-'||LINE_VERSION_NUMBER||'-'||SEQUENCE_EXTENSION||'-'||LINE_DLM is null


