
    
    



select count(*) as validation_errors
from (

    select
        CLAIM_NUMBER||RELATIONSHIP_END_DATE_KEY

    from EDW_STG_CLAIMS_MART.FLF_CLAIM_POLICY_ASSIGNMENT
    where CLAIM_NUMBER||RELATIONSHIP_END_DATE_KEY is not null
    group by CLAIM_NUMBER||RELATIONSHIP_END_DATE_KEY
    having count(*) > 1

) validation_errors


