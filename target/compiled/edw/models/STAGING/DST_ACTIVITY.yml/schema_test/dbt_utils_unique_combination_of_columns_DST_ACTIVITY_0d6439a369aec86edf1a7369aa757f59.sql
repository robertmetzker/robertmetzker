





with validation_errors as (

    select
        ACTV_ACTN_TYP_NM, ACTV_NM_TYP_NM, CNTX_TYP_NM, ACTV_DTL_COL_NM, SUBLOC_TYP_NM, ACTV_DTL_DESC
    from STAGING.DST_ACTIVITY

    group by ACTV_ACTN_TYP_NM, ACTV_NM_TYP_NM, CNTX_TYP_NM, ACTV_DTL_COL_NM, SUBLOC_TYP_NM, ACTV_DTL_DESC
    having count(*) > 1

)

select count(*)
from validation_errors


