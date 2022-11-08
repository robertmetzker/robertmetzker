
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_HOSPITAL_ICD_PROCEDURE
where ICD_PROCEDURE_CODE is null


