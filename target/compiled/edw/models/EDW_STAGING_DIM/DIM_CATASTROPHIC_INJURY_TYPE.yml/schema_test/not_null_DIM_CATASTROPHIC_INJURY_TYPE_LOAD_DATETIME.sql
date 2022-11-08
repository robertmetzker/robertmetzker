
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_CATASTROPHIC_INJURY_TYPE
where LOAD_DATETIME is null


