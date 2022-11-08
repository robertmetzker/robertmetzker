
    
    



select count(*) as validation_errors
from STAGING.DST_DRUG_PRICING_TYPE
where UNIQUE_ID_KEY is null


