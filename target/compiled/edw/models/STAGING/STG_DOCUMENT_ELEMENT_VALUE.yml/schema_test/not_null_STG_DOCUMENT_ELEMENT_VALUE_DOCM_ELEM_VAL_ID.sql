
    
    



select count(*) as validation_errors
from STAGING.STG_DOCUMENT_ELEMENT_VALUE
where DOCM_ELEM_VAL_ID is null


