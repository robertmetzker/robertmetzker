
    
    



select count(*) as validation_errors
from STAGING.DST_EARNED_PREMIUM_PAYMENTS
where UNIQUE_ID_KEY is null


