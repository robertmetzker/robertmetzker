
    
    



select count(*) as validation_errors
from (

    select
        INVOICE_NUMBER

    from STAGING.STG_INVOICE_HEADER
    where INVOICE_NUMBER is not null
    group by INVOICE_NUMBER
    having count(*) > 1

) validation_errors


