
    
    



select count(*) as validation_errors
from STAGING.DSV_PROVIDER_CERTIFICATION_STATUS_LOG
where BWC_CERTIFICATION_STATUS_DESC is null


