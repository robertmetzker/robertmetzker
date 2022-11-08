
    
    



select count(*) as validation_errors
from STAGING.DST_DOCUMENT_DETAIL
where UNIQUE_ID_KEY is null


