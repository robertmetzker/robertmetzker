
    
    



select count(*) as validation_errors
from STAGING.STG_THFICDP
where ICDV_CODE is null

