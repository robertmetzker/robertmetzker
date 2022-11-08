
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_OUTPATIENT_GROUPER
where UNIQUE_ID_KEY is null


