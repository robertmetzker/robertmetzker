
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HOSPITAL_ICD_PROCEDURE
where PRIMARY_SOURCE_SYSTEM is null


