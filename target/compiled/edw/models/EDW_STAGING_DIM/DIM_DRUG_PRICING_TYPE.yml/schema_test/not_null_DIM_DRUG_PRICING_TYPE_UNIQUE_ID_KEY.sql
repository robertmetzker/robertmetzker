
    
    



select count(*) as validation_errors
from EDW_STAGING_DIM.DIM_DRUG_PRICING_TYPE
where UNIQUE_ID_KEY is null


