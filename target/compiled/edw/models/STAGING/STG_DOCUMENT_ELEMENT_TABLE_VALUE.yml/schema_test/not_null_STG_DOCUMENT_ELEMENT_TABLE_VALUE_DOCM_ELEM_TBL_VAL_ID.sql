
    
    



select count(*) as validation_errors
from STAGING.STG_DOCUMENT_ELEMENT_TABLE_VALUE
where DOCM_ELEM_TBL_VAL_ID is null


