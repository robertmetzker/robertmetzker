
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_STATUS_HKEY

    from STAGING.DST_INVOICE_STATUS
    where INVOICE_STATUS_HKEY is not null
    group by INVOICE_STATUS_HKEY
    having count(*) > 1

) validation_errors


