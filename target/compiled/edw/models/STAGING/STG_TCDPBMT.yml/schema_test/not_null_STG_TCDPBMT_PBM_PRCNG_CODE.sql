
    
    



select count(*) as validation_errors
from STAGING.STG_TCDPBMT
where PBM_PRCNG_CODE is null


