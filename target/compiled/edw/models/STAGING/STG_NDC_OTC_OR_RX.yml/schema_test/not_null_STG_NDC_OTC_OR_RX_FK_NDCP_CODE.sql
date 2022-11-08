
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_OTC_OR_RX
where FK_NDCP_CODE is null


