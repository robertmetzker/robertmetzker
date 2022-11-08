
    
    



select count(*) as validation_errors
from STAGING.STG_BSDA_PLCY_FNCL_TRAN_XREF
where BILL_SCH_DTL_AMT_ID is null


