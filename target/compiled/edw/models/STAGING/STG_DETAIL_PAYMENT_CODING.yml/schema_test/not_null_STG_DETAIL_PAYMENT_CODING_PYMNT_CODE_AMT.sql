
    
    



select count(*) as validation_errors
from STAGING.STG_DETAIL_PAYMENT_CODING
where PYMNT_CODE_AMT is null


