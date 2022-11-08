
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_NMBR||EXTRACT_DATE

    from ODS_ACTUARIAL.BWC_STG_MIRA_RESERVE_EXTRACT
    where CLAIM_NMBR||EXTRACT_DATE is not null
    group by CLAIM_NMBR||EXTRACT_DATE
    having count(*) > 1

) validation_errors


