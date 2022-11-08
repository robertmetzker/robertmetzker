
    
    



select count(*) as validation_errors
from STAGING.STG_TMPPRDT
where PRVDR_ID||PRDT_CRT_DTTM||PRVDR_BASE_NMBR||PRVDR_SFX_NMBR is null


