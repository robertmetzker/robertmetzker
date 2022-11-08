
    
    



select count(*) as validation_errors
from (

    select
        EDI_INFO_ID

    from STAGING.STG_EDI_INFO
    where EDI_INFO_ID is not null
    group by EDI_INFO_ID
    having count(*) > 1

) validation_errors


