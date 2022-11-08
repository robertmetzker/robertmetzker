
    
    



select count(*) as validation_errors
from (

    select
        CARE824_RID

    from STAGING.STG_CARE824
    where CARE824_RID is not null
    group by CARE824_RID
    having count(*) > 1

) validation_errors


