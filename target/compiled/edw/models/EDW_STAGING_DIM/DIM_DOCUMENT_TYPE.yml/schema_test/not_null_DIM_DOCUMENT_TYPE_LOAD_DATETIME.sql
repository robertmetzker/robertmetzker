
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DOCUMENT_TYPE
where LOAD_DATETIME is null


