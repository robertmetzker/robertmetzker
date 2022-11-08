
    
    



select count(*) as validation_errors
from (

    select
        MEDISPAN_4_GPI_CLASS_CODE

    from STAGING.STG_BWC_DRUG_CATEGORY_REFERENCE
    where MEDISPAN_4_GPI_CLASS_CODE is not null
    group by MEDISPAN_4_GPI_CLASS_CODE
    having count(*) > 1

) validation_errors


