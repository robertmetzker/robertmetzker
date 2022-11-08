
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_MODIFIER_SEQUENCE
where LOAD_DATETIME is null


