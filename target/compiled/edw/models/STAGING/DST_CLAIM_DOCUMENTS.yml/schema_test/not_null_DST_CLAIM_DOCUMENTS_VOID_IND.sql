
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_DOCUMENTS
where VOID_IND is null


