
    
    



select count(*) as validation_errors
from STAGING.DSV_DRUG_PRICING_TYPE
where UNIQUE_ID_KEY is null


