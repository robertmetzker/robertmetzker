
    
    



select count(*) as validation_errors
from (

    select
        PYRL_RPT_ID

    from STAGING.STG_PAYROLL_REPORT
    where PYRL_RPT_ID is not null
    group by PYRL_RPT_ID
    having count(*) > 1

) validation_errors


