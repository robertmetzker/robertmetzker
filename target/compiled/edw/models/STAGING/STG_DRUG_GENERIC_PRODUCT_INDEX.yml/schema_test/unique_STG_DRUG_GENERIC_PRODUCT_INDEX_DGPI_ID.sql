
    
    



select count(*) as validation_errors
from (

    select
        DGPI_ID

    from STAGING.STG_DRUG_GENERIC_PRODUCT_INDEX
    where DGPI_ID is not null
    group by DGPI_ID
    having count(*) > 1

) validation_errors


