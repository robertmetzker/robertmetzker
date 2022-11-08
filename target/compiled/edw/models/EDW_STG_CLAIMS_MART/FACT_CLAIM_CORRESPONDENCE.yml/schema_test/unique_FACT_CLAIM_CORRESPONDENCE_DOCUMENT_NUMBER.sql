
    
    



select count(*) as validation_errors
from (

    select
        DOCUMENT_NUMBER

    from EDW_STG_CLAIMS_MART.FACT_CLAIM_CORRESPONDENCE
    where DOCUMENT_NUMBER is not null
    group by DOCUMENT_NUMBER
    having count(*) > 1

) validation_errors


