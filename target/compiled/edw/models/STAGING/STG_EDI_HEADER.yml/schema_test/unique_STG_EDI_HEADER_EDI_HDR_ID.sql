
    
    



select count(*) as validation_errors
from (

    select
        EDI_HDR_ID

    from STAGING.STG_EDI_HEADER
    where EDI_HDR_ID is not null
    group by EDI_HDR_ID
    having count(*) > 1

) validation_errors


