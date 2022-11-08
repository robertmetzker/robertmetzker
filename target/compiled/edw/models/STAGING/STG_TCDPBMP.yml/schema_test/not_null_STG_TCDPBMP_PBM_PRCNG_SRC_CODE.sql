
    
    



select count(*) as validation_errors
from STAGING.STG_TCDPBMP
where PBM_PRCNG_SRC_CODE is null


