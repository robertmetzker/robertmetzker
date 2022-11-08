





with validation_errors as (

    select
        BNFT_TYP_CD, BNFT_CTG_TYP_CD, BNFT_RPT_TYP_CD, BNFT_RPT_TYP_NM, JUR_TYP_CD
    from STAGING.STG_BENEFIT_TYPE

    group by BNFT_TYP_CD, BNFT_CTG_TYP_CD, BNFT_RPT_TYP_CD, BNFT_RPT_TYP_NM, JUR_TYP_CD
    having count(*) > 1

)

select count(*)
from validation_errors


