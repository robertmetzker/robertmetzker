





with validation_errors as (

    select
        CLM_TYP_CD, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD, CLM_TYP_NM, CLM_STT_TYP_NM, CLM_STS_TYP_NM, CLM_TRANS_RSN_TYP_NM
    from STAGING.STG_CLAIM_TYPE_STATUS

    group by CLM_TYP_CD, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD, CLM_TYP_NM, CLM_STT_TYP_NM, CLM_STS_TYP_NM, CLM_TRANS_RSN_TYP_NM
    having count(*) > 1

)

select count(*)
from validation_errors


