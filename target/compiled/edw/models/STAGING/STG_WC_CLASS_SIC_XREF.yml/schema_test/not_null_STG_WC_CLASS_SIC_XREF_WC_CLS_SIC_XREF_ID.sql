
    
    



select count(*) as validation_errors
from STAGING.STG_WC_CLASS_SIC_XREF
where WC_CLS_SIC_XREF_ID is null


