
    
    



select count(*) as validation_errors
from (

    select
        ICD_CD

    from STAGING.STG_BWC_ICD_ADDTNL_INFO
    where ICD_CD is not null
    group by ICD_CD
    having count(*) > 1

) validation_errors


