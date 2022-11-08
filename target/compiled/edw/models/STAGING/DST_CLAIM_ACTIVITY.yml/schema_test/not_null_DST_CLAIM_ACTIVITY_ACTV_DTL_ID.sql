
    
    



select count(*) as validation_errors
from STAGING.DST_CLAIM_ACTIVITY
where ACTV_DTL_ID is null


