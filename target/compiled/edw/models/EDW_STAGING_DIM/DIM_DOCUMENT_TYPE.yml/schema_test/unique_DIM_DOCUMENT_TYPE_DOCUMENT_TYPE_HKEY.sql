
    
    



select count(*) as validation_errors
from (

    select
        DOCUMENT_TYPE_HKEY

    from EDW_STAGING_DIM.DIM_DOCUMENT_TYPE
    where DOCUMENT_TYPE_HKEY is not null
    group by DOCUMENT_TYPE_HKEY
    having count(*) > 1

) validation_errors


