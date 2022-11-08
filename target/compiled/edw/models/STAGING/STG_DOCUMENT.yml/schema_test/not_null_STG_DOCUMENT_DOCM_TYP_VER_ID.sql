
    
    



select count(*) as validation_errors
from STAGING.STG_DOCUMENT
where DOCM_TYP_VER_ID is null


