
    
    



select count(*) as validation_errors
from (

    select
        DTVE_ID

    from STAGING.STG_DOCUMENT_TYPE_VERSION_ELEMENT
    where DTVE_ID is not null
    group by DTVE_ID
    having count(*) > 1

) validation_errors


