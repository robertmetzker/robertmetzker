
    
    



select count(*) as validation_errors
from (

    select
        CERT_AG_FNCL_TRAN_DTL_ID

    from STAGING.STG_CERTIFY_AG_FNCL_TRAN_DTL
    where CERT_AG_FNCL_TRAN_DTL_ID is not null
    group by CERT_AG_FNCL_TRAN_DTL_ID
    having count(*) > 1

) validation_errors


