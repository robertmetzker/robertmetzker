
    
    



select count(*) as validation_errors
from STAGING.DST_BWC_CALC_DRG_OUTPUT
where UNIQUE_ID_KEY is null


