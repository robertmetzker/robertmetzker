
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_MODIFIER_SEQUENCE
where PRIMARY_SOURCE_SYSTEM is null


