
    
    



select count(*) as validation_errors
from STAGING.DST_POLICY_BILLING
where EMPLOYEE_LEASING_TYPE_CODE is null


