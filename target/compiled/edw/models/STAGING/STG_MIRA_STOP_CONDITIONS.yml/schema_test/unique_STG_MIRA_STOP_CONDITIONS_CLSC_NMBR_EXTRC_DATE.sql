
    
    



select count(*) as validation_errors
from (

    select
        CLSC_NMBR||EXTRC_DATE

    from STAGING.STG_MIRA_STOP_CONDITIONS
    where CLSC_NMBR||EXTRC_DATE is not null
    group by CLSC_NMBR||EXTRC_DATE
    having count(*) > 1

) validation_errors


