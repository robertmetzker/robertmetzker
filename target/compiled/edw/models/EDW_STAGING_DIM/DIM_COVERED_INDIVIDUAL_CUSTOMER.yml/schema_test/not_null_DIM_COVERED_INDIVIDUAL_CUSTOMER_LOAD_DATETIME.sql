
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_COVERED_INDIVIDUAL_CUSTOMER
where LOAD_DATETIME is null


