
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_PRESCRIPTION_BILL
where UNIQUE_ID_KEY is null


