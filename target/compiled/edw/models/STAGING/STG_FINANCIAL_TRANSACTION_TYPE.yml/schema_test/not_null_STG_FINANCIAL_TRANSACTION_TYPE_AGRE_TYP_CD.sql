
    
    



select count(*) as validation_errors
from STAGING.STG_FINANCIAL_TRANSACTION_TYPE
where AGRE_TYP_CD is null


