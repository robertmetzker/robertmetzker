
    
    



select count(*) as validation_errors
from STAGING.STG_MIRA_STOP_CONDITIONS
where CLSC_NMBR||EXTRC_DATE is null


