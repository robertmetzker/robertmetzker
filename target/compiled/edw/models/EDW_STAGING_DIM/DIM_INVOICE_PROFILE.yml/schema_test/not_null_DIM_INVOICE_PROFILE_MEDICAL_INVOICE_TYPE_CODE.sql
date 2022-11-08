
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_INVOICE_PROFILE
where MEDICAL_INVOICE_TYPE_CODE is null


