
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INVOICE_STATUS
where PRIMARY_SOURCE_SYSTEM is null


