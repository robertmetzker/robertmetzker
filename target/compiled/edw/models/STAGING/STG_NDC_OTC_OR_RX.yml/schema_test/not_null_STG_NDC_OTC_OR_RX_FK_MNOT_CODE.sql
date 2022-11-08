
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_OTC_OR_RX
where FK_MNOT_CODE is null


