
    
    



select count(*) as validation_errors
from STAGING.STG_CERTIFY_AG_FNCL_TRAN_DTL
where CERT_AG_FNCL_TRAN_DTL_ID is null


