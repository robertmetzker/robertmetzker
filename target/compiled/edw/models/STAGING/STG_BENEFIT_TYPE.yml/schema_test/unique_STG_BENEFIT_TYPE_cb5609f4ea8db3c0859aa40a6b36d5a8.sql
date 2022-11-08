
    
    



select count(*) as validation_errors
from (

    select
        BNFT_TYP_CD||BNFT_CTG_TYP_CD||BNFT_RPT_TYP_CD||BNFT_RPT_TYP_NM||JUR_TYP_CD

    from STAGING.STG_BENEFIT_TYPE
    where BNFT_TYP_CD||BNFT_CTG_TYP_CD||BNFT_RPT_TYP_CD||BNFT_RPT_TYP_NM||JUR_TYP_CD is not null
    group by BNFT_TYP_CD||BNFT_CTG_TYP_CD||BNFT_RPT_TYP_CD||BNFT_RPT_TYP_NM||JUR_TYP_CD
    having count(*) > 1

) validation_errors


