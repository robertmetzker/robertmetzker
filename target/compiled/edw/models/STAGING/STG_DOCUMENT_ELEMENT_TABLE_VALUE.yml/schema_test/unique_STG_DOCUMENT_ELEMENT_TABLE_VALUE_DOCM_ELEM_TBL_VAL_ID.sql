
    
    



select count(*) as validation_errors
from (

    select
        DOCM_ELEM_TBL_VAL_ID

    from STAGING.STG_DOCUMENT_ELEMENT_TABLE_VALUE
    where DOCM_ELEM_TBL_VAL_ID is not null
    group by DOCM_ELEM_TBL_VAL_ID
    having count(*) > 1

) validation_errors


