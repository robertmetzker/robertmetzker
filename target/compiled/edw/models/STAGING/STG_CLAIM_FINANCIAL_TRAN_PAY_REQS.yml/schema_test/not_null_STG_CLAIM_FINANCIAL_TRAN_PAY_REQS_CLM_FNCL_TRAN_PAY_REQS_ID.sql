
    
    



select count(*) as validation_errors
from STAGING.STG_CLAIM_FINANCIAL_TRAN_PAY_REQS
where CLM_FNCL_TRAN_PAY_REQS_ID is null


