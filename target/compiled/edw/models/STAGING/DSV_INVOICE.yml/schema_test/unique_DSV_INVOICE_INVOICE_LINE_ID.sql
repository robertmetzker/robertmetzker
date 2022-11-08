
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_LINE_ID

    from STAGING.DSV_INVOICE
    where INVOICE_LINE_ID is not null
    group by INVOICE_LINE_ID
    having count(*) > 1

) validation_errors


