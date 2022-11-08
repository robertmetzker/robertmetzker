
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_NUMBER||EXTRC_DATE

    from STAGING.STG_MIRA_VALIDATION_EXTRACT
    where CLAIM_NUMBER||EXTRC_DATE is not null
    group by CLAIM_NUMBER||EXTRC_DATE
    having count(*) > 1

) validation_errors


