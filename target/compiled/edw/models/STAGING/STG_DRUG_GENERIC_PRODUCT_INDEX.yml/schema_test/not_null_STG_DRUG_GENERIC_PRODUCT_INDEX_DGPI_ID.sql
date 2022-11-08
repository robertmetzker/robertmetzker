
    
    



select count(*) as validation_errors
from STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX
where DGPI_ID is null


