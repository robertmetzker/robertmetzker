
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DOCUMENT_TYPE
where DOCUMENT_TYPE_HKEY is null


