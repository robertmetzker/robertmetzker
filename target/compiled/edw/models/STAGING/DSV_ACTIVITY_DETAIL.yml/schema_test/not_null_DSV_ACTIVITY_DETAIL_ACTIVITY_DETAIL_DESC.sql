
    
    



select count(*) as validation_errors
from STAGING.DSV_ACTIVITY_DETAIL
where ACTIVITY_DETAIL_DESC is null


