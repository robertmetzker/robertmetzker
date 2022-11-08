
    
    



select count(*) as validation_errors
from (

    select
        DOCUMENT_DETAIL_HKEY

    from EDW_STAGING_DIM.DIM_DOCUMENT_DETAIL
    where DOCUMENT_DETAIL_HKEY is not null
    group by DOCUMENT_DETAIL_HKEY
    having count(*) > 1

) validation_errors


