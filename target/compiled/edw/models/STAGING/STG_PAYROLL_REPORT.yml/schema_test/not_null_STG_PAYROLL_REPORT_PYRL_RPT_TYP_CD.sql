
    
    



select count(*) as validation_errors
from STAGING.STG_PAYROLL_REPORT
where PYRL_RPT_TYP_CD is null


