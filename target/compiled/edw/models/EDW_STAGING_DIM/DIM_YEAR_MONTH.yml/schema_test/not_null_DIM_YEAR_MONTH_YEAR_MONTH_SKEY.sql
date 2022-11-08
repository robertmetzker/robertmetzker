
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_YEAR_MONTH
where YEAR_MONTH_SKEY is null


