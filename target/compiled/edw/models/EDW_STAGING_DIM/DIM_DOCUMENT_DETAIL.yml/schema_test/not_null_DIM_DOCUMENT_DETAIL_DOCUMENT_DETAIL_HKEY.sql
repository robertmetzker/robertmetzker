
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DOCUMENT_DETAIL
where DOCUMENT_DETAIL_HKEY is null


