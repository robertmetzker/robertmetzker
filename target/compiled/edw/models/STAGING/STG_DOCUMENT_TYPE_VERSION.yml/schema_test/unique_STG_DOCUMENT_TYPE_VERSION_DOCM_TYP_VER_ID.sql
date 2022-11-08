
    
    



select count(*) as validation_errors
from (

    select
        DOCM_TYP_VER_ID

    from STAGING.STG_DOCUMENT_TYPE_VERSION
    where DOCM_TYP_VER_ID is not null
    group by DOCM_TYP_VER_ID
    having count(*) > 1

) validation_errors


