
    
    



select count(*) as validation_errors
from (

    select
        PRVDR_ID||PRDT_CRT_DTTM||PRVDR_BASE_NMBR||PRVDR_SFX_NMBR

    from STAGING.STG_TMPPRDT
    where PRVDR_ID||PRDT_CRT_DTTM||PRVDR_BASE_NMBR||PRVDR_SFX_NMBR is not null
    group by PRVDR_ID||PRDT_CRT_DTTM||PRVDR_BASE_NMBR||PRVDR_SFX_NMBR
    having count(*) > 1

) validation_errors


