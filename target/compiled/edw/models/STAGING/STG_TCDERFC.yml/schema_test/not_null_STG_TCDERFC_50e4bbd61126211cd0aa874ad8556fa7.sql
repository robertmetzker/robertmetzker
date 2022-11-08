
    
    



select count(*) as validation_errors
from STAGING.STG_TCDERFC
where ICD_CODE||ICDV_CODE||EM_RFRL_BGNG_DATE is null


