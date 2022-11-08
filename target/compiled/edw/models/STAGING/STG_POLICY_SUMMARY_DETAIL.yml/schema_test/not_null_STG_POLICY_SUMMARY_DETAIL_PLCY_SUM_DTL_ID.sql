
    
    



select count(*) as validation_errors
from STAGING.STG_POLICY_SUMMARY_DETAIL
where PLCY_SUM_DTL_ID is null


