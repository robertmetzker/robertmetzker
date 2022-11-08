
    
    



select count(*) as validation_errors
from (

    select
        DOCM_ID

    from STAGING.DST_CLAIM_DOCUMENTS
    where DOCM_ID is not null
    group by DOCM_ID
    having count(*) > 1

) validation_errors


