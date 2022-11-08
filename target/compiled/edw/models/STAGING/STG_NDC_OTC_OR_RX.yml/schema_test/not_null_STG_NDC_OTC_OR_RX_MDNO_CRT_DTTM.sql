
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_OTC_OR_RX
where MDNO_CRT_DTTM is null


