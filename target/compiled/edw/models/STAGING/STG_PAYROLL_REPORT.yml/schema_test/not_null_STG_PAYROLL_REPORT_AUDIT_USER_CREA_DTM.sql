
    
    



select count(*) as validation_errors
from STAGING.STG_PAYROLL_REPORT
where AUDIT_USER_CREA_DTM is null


