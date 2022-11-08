
    
    



select count(*) as validation_errors
from (

    select
        ICD_CODE||ICDV_CODE||EM_RFRL_BGNG_DATE

    from STAGING.STG_TCDERFC
    where ICD_CODE||ICDV_CODE||EM_RFRL_BGNG_DATE is not null
    group by ICD_CODE||ICDV_CODE||EM_RFRL_BGNG_DATE
    having count(*) > 1

) validation_errors


