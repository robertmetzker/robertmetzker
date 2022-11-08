
    
    



select count(*) as validation_errors
from STAGING.DSV_PBM_PRICING_METHOD
where UNIQUE_ID_KEY is null


