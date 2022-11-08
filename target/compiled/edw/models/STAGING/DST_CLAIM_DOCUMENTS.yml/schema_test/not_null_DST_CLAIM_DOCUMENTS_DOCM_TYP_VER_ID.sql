
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_DOCUMENTS
where DOCM_TYP_VER_ID is null


