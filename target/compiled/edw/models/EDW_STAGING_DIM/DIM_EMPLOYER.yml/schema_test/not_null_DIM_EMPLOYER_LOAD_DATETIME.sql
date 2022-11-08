
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_EMPLOYER
where LOAD_DATETIME is null


