
    
    



select count(*) as validation_errors
from STAGING.DST_NDC_DRUG_PRICING
where NDC_11_CODE is null


