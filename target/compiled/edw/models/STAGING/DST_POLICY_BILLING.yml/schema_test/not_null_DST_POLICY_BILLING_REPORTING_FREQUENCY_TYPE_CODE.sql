
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_BILLING
where REPORTING_FREQUENCY_TYPE_CODE is null


