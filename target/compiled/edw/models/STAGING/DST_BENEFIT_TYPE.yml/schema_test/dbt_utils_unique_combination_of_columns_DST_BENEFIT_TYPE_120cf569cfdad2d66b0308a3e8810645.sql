





with validation_errors as (

    select
        BNFT_TYP_CD, JUR_TYP_CD, BNFT_RPT_TYP_NM
    from STAGING.DST_BENEFIT_TYPE

    group by BNFT_TYP_CD, JUR_TYP_CD, BNFT_RPT_TYP_NM
    having count(*) > 1

)

select count(*)
from validation_errors


