
    
    



select count(*) as validation_errors
from STAGING.STG_INVOICE_LINE
where LINE_SEQUENCE is null

