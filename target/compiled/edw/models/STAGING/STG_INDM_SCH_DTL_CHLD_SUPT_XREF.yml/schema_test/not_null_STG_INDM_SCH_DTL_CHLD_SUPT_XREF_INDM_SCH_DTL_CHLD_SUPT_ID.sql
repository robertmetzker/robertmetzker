
    
    



select count(*) as validation_errors
from STAGING.STG_INDM_SCH_DTL_CHLD_SUPT_XREF
where INDM_SCH_DTL_CHLD_SUPT_ID is null


