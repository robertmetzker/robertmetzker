
    
    



select count(*) as validation_errors
from STAGING.STG_APC_COMPOSITE_ADJUSTMENT
where APC_COMPOSITE_ADJUSTMENT_CODE is null


