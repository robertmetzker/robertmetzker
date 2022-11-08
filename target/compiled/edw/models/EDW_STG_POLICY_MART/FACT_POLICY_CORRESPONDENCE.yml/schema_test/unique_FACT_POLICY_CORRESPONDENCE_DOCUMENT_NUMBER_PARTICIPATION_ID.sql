
    
    



select count(*) as validation_errors
from (

    select
        DOCUMENT_NUMBER || PARTICIPATION_ID

    from EDW_STG_POLICY_MART.FACT_POLICY_CORRESPONDENCE
    where DOCUMENT_NUMBER || PARTICIPATION_ID is not null
    group by DOCUMENT_NUMBER || PARTICIPATION_ID
    having count(*) > 1

) validation_errors


