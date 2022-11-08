
    
    



select count(*) as validation_errors
from STAGING.DST_EOB
where EOB_RID is null


