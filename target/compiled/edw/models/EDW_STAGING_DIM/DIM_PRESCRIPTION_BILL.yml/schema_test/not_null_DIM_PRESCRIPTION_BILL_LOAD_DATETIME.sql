
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PRESCRIPTION_BILL
where LOAD_DATETIME is null


