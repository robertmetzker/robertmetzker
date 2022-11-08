
    
    



select count(*) as validation_errors
from (

    select
        WC_CLS_SUFX_ID

    from STAGING.STG_WC_CLASS
    where WC_CLS_SUFX_ID is not null
    group by WC_CLS_SUFX_ID
    having count(*) > 1

) validation_errors


