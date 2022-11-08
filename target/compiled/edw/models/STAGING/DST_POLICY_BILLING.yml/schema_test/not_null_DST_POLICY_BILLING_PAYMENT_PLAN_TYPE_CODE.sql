
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_BILLING
where PAYMENT_PLAN_TYPE_CODE is null


