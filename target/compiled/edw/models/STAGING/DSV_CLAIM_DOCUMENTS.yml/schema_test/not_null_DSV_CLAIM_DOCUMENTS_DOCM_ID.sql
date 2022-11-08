
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_DOCUMENTS
where DOCM_ID is null


