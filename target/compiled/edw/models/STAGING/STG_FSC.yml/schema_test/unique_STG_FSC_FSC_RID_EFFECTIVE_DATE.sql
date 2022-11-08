
    
    



select count(*) as validation_errors
from (

    select
        FSC_RID||EFFECTIVE_DATE

    from STAGING.STG_FSC
    where FSC_RID||EFFECTIVE_DATE is not null
    group by FSC_RID||EFFECTIVE_DATE
    having count(*) > 1

) validation_errors


