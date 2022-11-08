
    
    



select count(*) as validation_errors
from (

    select
        CASE_NUMBER

    from EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE
    where CASE_NUMBER is not null
    group by CASE_NUMBER
    having count(*) > 1

) validation_errors


