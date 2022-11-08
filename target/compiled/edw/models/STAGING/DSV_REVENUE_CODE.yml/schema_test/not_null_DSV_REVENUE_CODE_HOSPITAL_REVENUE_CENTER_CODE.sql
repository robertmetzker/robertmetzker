
    
    



select count(*) as validation_errors
from STAGING.DSV_REVENUE_CODE
where HOSPITAL_REVENUE_CENTER_CODE is null


