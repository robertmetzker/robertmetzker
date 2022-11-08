
    
    



select count(*) as validation_errors
from STAGING.STG_BWC_DRUG_CATEGORY_REFERENCE
where MEDISPAN_4_GPI_CLASS_NAME is null


