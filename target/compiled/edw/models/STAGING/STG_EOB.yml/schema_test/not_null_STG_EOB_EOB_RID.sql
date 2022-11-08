
    
    



select count(*) as validation_errors
from STAGING.STG_EOB
where EOB_RID is null


