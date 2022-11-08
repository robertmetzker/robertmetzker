

---- SRC LAYER ----
WITH

SRC_CLM as ( SELECT *     from     STAGING.DSV_CLAIM_ICD_STATUS_HISTORY ),
SRC_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
//SRC_CLM as ( SELECT *     from     STAGING.DSV_CLAIM_ICD_STATUS_HISTORY) ,
//SRC_ICD as ( SELECT *     from     DIM_ICD) ,

---- LOGIC LAYER ----


LOGIC_CLM as ( SELECT 
		  HIST_ID                                         as                                                HIST_ID 
		,  md5(cast(
    
    coalesce(cast(ICD_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_ICD_STS_PRI_IND as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                          CLAIM_ICD_STATUS_TYPE_HKEY 
		, ICD_STS_DT                                         as                                          ICD_STS_DT 
		, CASE WHEN ICD_STS_DT is null then '-1' 
			WHEN ICD_STS_DT < '1901-01-01' then '-2' 
			WHEN ICD_STS_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( ICD_STS_DT, '[^0-9]+', '') 
				END :: INTEGER                              as                                  ICD_STATUS_DATE_KEY
		, CASE WHEN nullif(array_to_string(array_construct_compact(CLM_TYP_CD, CHNG_OVR_IND, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD),''), '') is NULL  
				THEN MD5( '99999' ) ELSE
				md5(cast(
    
    coalesce(cast(CLM_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CHNG_OVR_IND as 
    varchar
), '') || '-' || coalesce(cast(CLM_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_TRANS_RSN_TYP_CD as 
    varchar
), '')

 as 
    varchar
))
				END                                          as                             CLAIM_TYPE_STATUS_HKEY 
		,  CASE WHEN nullif(array_to_string(array_construct_compact(OCCR_SRC_TYP_NM,OCCR_MEDA_TYP_NM,NOI_CTG_TYP_NM,NOI_TYP_NM,FIREFIGHTER_CANCER_IND,COVID_EXPOSURE_IND,COVID_EMERGENCY_WORKER_IND,COVID_HEALTH_CARE_WORKER_IND,COMBINED_CLAIM_IND,SB223_IND,EMPLOYER_PREMISES_IND,CLM_CTRPH_INJR_IND,K_PROGRAM_ENROLLMENT_DESC,K_PROGRAM_TYPE_DESC,K_PROGRAM_REASON_DESC),''), '') is NULL  
				THEN MD5( '99999' ) ELSE
				md5(cast(
    
    coalesce(cast(OCCR_SRC_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(OCCR_MEDA_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(NOI_CTG_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(NOI_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_CLAIM_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CLM_CTRPH_INJR_IND as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_ENROLLMENT_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(K_PROGRAM_REASON_DESC as 
    varchar
), '')

 as 
    varchar
))
				END                                          as                                  CLAIM_DETAIL_HKEY 
		, CLM_NO                                             as                                             CLM_NO 
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM 
		, OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM 
		, NOI_CTG_TYP_NM                                     as                                     NOI_CTG_TYP_NM 
		, NOI_TYP_NM                                         as                                         NOI_TYP_NM 
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND 
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND 
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND 
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND 
		, COMBINED_CLAIM_IND                                 as                                 COMBINED_CLAIM_IND 
		, SB223_IND                                          as                                          SB223_IND 
		, OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM 
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND 
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND 
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC 
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC 
		, K_PROGRAM_START_DATE                               as                               K_PROGRAM_START_DATE 
		, K_PROGRAM_END_DATE                                 as                                 K_PROGRAM_END_DATE 
		, K_PROGRAM_REASON_CODE                              as                              K_PROGRAM_REASON_CODE 
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
		, ICD_CD                                             as                                             ICD_CD 
		, ICD_VER_CD                                         as                                         ICD_VER_CD 
		, VOID_IND                                           as                                           VOID_IND 
		from SRC_CLM
            ),

LOGIC_ICD as ( SELECT 
		  ICD_HKEY                                           as                                           ICD_HKEY 
	    , ICD_CODE                                           as                                           ICD_CODE 
	    , RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		from SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  HIST_ID                                            as                                         HISTORY_ID
		, CLAIM_ICD_STATUS_TYPE_HKEY                         as                         CLAIM_ICD_STATUS_TYPE_HKEY
		, ICD_STS_DT                                         as                                         ICD_STS_DT
		, ICD_STATUS_DATE_KEY                                as                                ICD_STATUS_DATE_KEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM
		, OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM
		, NOI_CTG_TYP_NM                                     as                                     NOI_CTG_TYP_NM
		, NOI_TYP_NM                                         as                                         NOI_TYP_NM
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND
		, COMBINED_CLAIM_IND                                 as                                 COMBINED_CLAIM_IND
		, SB223_IND                                          as                                          SB223_IND
		, OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC
		, K_PROGRAM_START_DATE                               as                               K_PROGRAM_START_DATE
		, K_PROGRAM_END_DATE                                 as                                 K_PROGRAM_END_DATE
		, K_PROGRAM_REASON_CODE                              as                              K_PROGRAM_REASON_CODE
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC
		, ICD_CD                                             as                                             ICD_CD
		, ICD_VER_CD                                         as                                         ICD_VER_CD
		, VOID_IND                                           as                                           VOID_IND
				FROM     LOGIC_CLM   ), 
RENAME_ICD as ( SELECT 
		  ICD_HKEY                                           as                                           ICD_HKEY 
	    , ICD_CODE                                           as                                           ICD_CODE 
	    , RECORD_EFFECTIVE_DATE                              as                                 ICD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                       ICD_END_DATE
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM 
                                            WHERE VOID_IND = 'N'  ),
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),

---- JOIN LAYER ----

MD as ( SELECT * 
				FROM  FILTER_CLM
				LEFT JOIN FILTER_ICD ON  coalesce( FILTER_ICD.ICD_CODE,  'UNK') =  FILTER_CLM.ICD_CD AND FILTER_CLM.ICD_STS_DT BETWEEN FILTER_ICD.ICD_EFFECTIVE_DATE AND coalesce( FILTER_ICD.ICD_END_DATE, '2099-12-31')  ),

-- ETL Layer---

ETL AS (
SELECT  
  HISTORY_ID
, CLAIM_ICD_STATUS_TYPE_HKEY
, ICD_STATUS_DATE_KEY
, CLAIM_TYPE_STATUS_HKEY
, CLAIM_DETAIL_HKEY
, coalesce( ICD_HKEY, MD5( '-1111' )) as  ICD_HKEY
, CLAIM_NUMBER
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
from MD
)

SELECT * from ETL