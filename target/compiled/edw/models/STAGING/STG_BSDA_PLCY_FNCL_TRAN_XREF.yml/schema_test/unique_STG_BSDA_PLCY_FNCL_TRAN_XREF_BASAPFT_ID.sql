
    
    



select count(*) as validation_errors
from (

    select
        BASAPFT_ID

    from STAGING.STG_BSDA_PLCY_FNCL_TRAN_XREF
    where BASAPFT_ID is not null
    group by BASAPFT_ID
    having count(*) > 1

) validation_errors


