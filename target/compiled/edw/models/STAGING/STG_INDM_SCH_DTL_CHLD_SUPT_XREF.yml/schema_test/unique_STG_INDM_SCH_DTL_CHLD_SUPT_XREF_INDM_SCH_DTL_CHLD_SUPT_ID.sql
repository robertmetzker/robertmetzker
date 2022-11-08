
    
    



select count(*) as validation_errors
from (

    select
        INDM_SCH_DTL_CHLD_SUPT_ID

    from STAGING.STG_INDM_SCH_DTL_CHLD_SUPT_XREF
    where INDM_SCH_DTL_CHLD_SUPT_ID is not null
    group by INDM_SCH_DTL_CHLD_SUPT_ID
    having count(*) > 1

) validation_errors


