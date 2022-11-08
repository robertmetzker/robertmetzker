
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_HEADER_ID

    from STAGING.STG_INVOICE_HEADER
    where INVOICE_HEADER_ID is not null
    group by INVOICE_HEADER_ID
    having count(*) > 1

) validation_errors


