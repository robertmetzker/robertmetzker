
    
    



select count(*) as validation_errors
from (

    select
        INV_INVOICE_LINE_ID

    from STAGING.DST_MEDICAL_INVOICE_LINE_EDIT_EOB
    where INV_INVOICE_LINE_ID is not null
    group by INV_INVOICE_LINE_ID
    having count(*) > 1

) validation_errors


