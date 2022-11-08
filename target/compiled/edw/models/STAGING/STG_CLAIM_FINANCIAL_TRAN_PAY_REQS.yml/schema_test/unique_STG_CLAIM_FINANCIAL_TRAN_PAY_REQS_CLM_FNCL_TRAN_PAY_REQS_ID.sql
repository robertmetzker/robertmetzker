
    
    



select count(*) as validation_errors
from (

    select
        CLM_FNCL_TRAN_PAY_REQS_ID

    from STAGING.STG_CLAIM_FINANCIAL_TRAN_PAY_REQS
    where CLM_FNCL_TRAN_PAY_REQS_ID is not null
    group by CLM_FNCL_TRAN_PAY_REQS_ID
    having count(*) > 1

) validation_errors


