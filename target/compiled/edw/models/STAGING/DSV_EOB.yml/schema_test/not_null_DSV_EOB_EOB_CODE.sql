
    
    



select count(*) as validation_errors
from STAGING.DSV_EOB
where EOB_CODE is null


