
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_YEAR_CONTROL_ELEMENT
where LOAD_DATETIME is null


