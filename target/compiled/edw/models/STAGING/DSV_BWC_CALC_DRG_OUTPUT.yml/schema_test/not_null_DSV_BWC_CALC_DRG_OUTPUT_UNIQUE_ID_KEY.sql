
    
    



select count(*) as validation_errors
from STAGING.DSV_BWC_CALC_DRG_OUTPUT
where UNIQUE_ID_KEY is null


