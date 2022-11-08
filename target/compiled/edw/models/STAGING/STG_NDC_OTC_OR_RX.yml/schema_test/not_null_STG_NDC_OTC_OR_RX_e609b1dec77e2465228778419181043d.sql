
    
    



select count(*) as validation_errors
from STAGING.STG_NDC_OTC_OR_RX
where FK_NDCL_CODE||FK_DCPR_CODE||FK_NDCP_CODE||FK_NDCV_NMBR||FK_MNOT_CODE||MDNO_CRT_DTTM is null


