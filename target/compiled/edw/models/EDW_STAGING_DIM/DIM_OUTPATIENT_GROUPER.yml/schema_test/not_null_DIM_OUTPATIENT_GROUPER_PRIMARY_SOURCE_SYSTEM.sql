
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER
where PRIMARY_SOURCE_SYSTEM is null


