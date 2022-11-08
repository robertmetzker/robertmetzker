
    
    



select count(*) as validation_errors
from (

    select
        FK_NDCL_CODE||'-'||FK_DCPR_CODE||'-'||FK_NDCP_CODE||'-'||FK_NDCV_NMBR||'-'||FK_MGPC_NMBR||'-'||NVMG_CRT_DTTM

    from STAGING.STG_DRUG_PACKAGE
    where FK_NDCL_CODE||'-'||FK_DCPR_CODE||'-'||FK_NDCP_CODE||'-'||FK_NDCV_NMBR||'-'||FK_MGPC_NMBR||'-'||NVMG_CRT_DTTM is not null
    group by FK_NDCL_CODE||'-'||FK_DCPR_CODE||'-'||FK_NDCP_CODE||'-'||FK_NDCV_NMBR||'-'||FK_MGPC_NMBR||'-'||NVMG_CRT_DTTM
    having count(*) > 1

) validation_errors


