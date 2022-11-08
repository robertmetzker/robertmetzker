
    
    



select count(*) as validation_errors
from STAGING.STG_DOCUMENT_TYPE_VERSION
where DOCM_TYP_VER_ID is null


