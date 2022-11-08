
    
    



select count(*) as validation_errors
from STAGING.STG_BWC_DRUG_MILLIGRAM_EQUIVALENCE_REFERENCE
where MEDISPAN_14_GPI_CODE is null


