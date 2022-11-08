
    
    



select count(*) as validation_errors
from STAGING.DSV_CLAIM_PAYMENT_SUMMARY
where INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT is null


