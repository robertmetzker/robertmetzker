
    
    



select count(*) as validation_errors
from STAGING.DST_EARNED_PREMIUM_PAYMENTS
where PFTA_AMT is null


