

---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.DSV_CLAIM_ICD_STATUS_HISTORY ),
SRC_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_MC as ( SELECT *     from     EDW_STAGING_DIM.DIM_MANUAL_CLASSIFICATION ),
//SRC_CLM as ( SELECT *     from     DSV_CLAIM_ICD_STATUS_HISTORY) ,
//SRC_ICD as ( SELECT *     from     DIM_ICD) ,
//SRC_MC as ( SELECT *     from     DIM_MANUAL_CLASSIFICATION) ,

---- LOGIC LAYER ----

LOGIC_CLM as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, CLM_NO                                             as                                             CLM_NO 
        ,  CASE WHEN CLM_ICD_DESC IS NULL THEN md5('99999')
		   ELSE md5(cast(
    
    coalesce(cast(CLM_ICD_DESC as 
    varchar
), '')

 as 
    varchar
))                                  
													END		 as                       CLAIM_ICD_SPECIFIC_DESC_HKEY 
		,  CASE WHEN CLAIM_INITIAL_FILE_DATE is null then '-1' 
			WHEN CLAIM_INITIAL_FILE_DATE < '1901-01-01' then '-2' 
			WHEN CLAIM_INITIAL_FILE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLAIM_INITIAL_FILE_DATE, '[^0-9]+', '') 
				END :: INTEGER                               as                            CLAIM_INITIAL_FILE_DATE
		, CASE WHEN CLAIM_FILE_DATE is null then '-1' 
			WHEN CLAIM_FILE_DATE < '1901-01-01' then '-2' 
			WHEN CLAIM_FILE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLAIM_FILE_DATE, '[^0-9]+', '') 
				END :: INTEGER                               as                                    CLAIM_FILE_DATE
		, CASE WHEN CLM_OCCR_DATE is null then '-1' 
			WHEN CLM_OCCR_DATE < '1901-01-01' then '-2' 
			WHEN CLM_OCCR_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_OCCR_DATE, '[^0-9]+', '') 
				END :: INTEGER                               as                          CLAIM_OCCURRENCE_DATE_KEY
		, CLM_OCCR_DATE                                     as 		                                 CLM_OCCR_DATE
		, CASE WHEN ICD_STS_DT is null then '-1' 
			WHEN ICD_STS_DT < '1901-01-01' then '-2' 
			WHEN ICD_STS_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( ICD_STS_DT, '[^0-9]+', '') 
				END :: INTEGER                               as                           CLAIM_ICD_STATUS_DATE_KEY
		,  md5(cast(
    
    coalesce(cast(ICD_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ICD_LOC_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(ICD_SITE_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_ICD_STS_PRI_IND as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_ICD_IND as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                       CLAIM_ICD_STATUS_DETAIL_HKEY                                                
		, CASE WHEN CLM_ICD_STS_EFF_DT is null then '-1' 
			WHEN CLM_ICD_STS_EFF_DT < '1901-01-01' then '-2' 
			WHEN CLM_ICD_STS_EFF_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_ICD_STS_EFF_DT, '[^0-9]+', '') 
				END :: INTEGER                               as                                        CLM_ICD_STS_EFF_DT
		, CASE WHEN CLM_ICD_STS_END_DT is null then '-1' 
			WHEN CLM_ICD_STS_END_DT < '1901-01-01' then '-2' 
			WHEN CLM_ICD_STS_END_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_ICD_STS_END_DT, '[^0-9]+', '') 
				END :: INTEGER                               as                                         CLM_ICD_STS_END_DT
		, CASE WHEN nullif(array_to_string(array_construct_compact( CLM_TYP_CD, CHNG_OVR_IND, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD ),''), '') is NULL  
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
                END                                          as                              CLAIM_TYPE_STATUS_HKEY		                                                   
        , CASE WHEN nullif(array_to_string(array_construct_compact( OCCR_SRC_TYP_NM,OCCR_MEDA_TYP_NM,NOI_CTG_TYP_NM,NOI_TYP_NM,FIREFIGHTER_CANCER_IND,COVID_EXPOSURE_IND,COVID_EMERGENCY_WORKER_IND,COVID_HEALTH_CARE_WORKER_IND,COMBINED_CLAIM_IND,SB223_IND,EMPLOYER_PREMISES_IND,CLM_CTRPH_INJR_IND,K_PROGRAM_ENROLLMENT_DESC,K_PROGRAM_TYPE_DESC, K_PROGRAM_REASON_DESC ),''), '') is NULL  
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
                   END                                       as                                  CLAIM_DETAIL_HKEY	                                              
		, CASE WHEN nullif(array_to_string(array_construct_compact( INDUSTRY_GROUP_CODE),''), '') is NULL  
                THEN MD5( '99999' ) ELSE  
                  md5(cast(
    
    coalesce(cast(INDUSTRY_GROUP_CODE as 
    varchar
), '')

 as 
    varchar
)) 
                   END                                       as                                INDUSTRY_GROUP_CODE	                                          
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
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD 
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD 
		, CURRENT_ICD_IND                                    as                                    CURRENT_ICD_IND 
		, CS_CLS_CD                                          as                                          CS_CLS_CD 
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE 
		, VOID_IND 											 as                                           VOID_IND 
        , ICD_STS_DT                                         as                                     CLM_ICD_STS_DT 
		from SRC_CLM
            ),
LOGIC_ICD as ( SELECT 
		  ICD_HKEY                                           as                                           ICD_HKEY 
		, ICD_CODE                                           as                                           ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		FROM SRC_ICD
            ),
LOGIC_MC as ( SELECT 
		  MANUAL_CLASS_HKEY                                  as                                  MANUAL_CLASS_HKEY 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
        , MANUAL_CLASS_CODE                                  as                                  MANUAL_CLASS_CODE
        , MANUAL_CLASS_SUFFIX_CODE                           as                           MANUAL_CLASS_SUFFIX_CODE
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		FROM SRC_MC
            )

---- RENAME LAYER ----
,

RENAME_CLM as ( SELECT 
		  HIST_ID                                            as                                         HISTORY_ID
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, CLAIM_ICD_SPECIFIC_DESC_HKEY                       as                       CLAIM_ICD_SPECIFIC_DESC_HKEY
		, CLAIM_INITIAL_FILE_DATE                            as                         CLAIM_INITAL_FILE_DATE_KEY
		, CLAIM_FILE_DATE                                    as                                CLAIM_FILE_DATE_KEY
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, CLAIM_OCCURRENCE_DATE_KEY                          as                          CLAIM_OCCURRENCE_DATE_KEY
		, CLAIM_ICD_STATUS_DATE_KEY                          as                          CLAIM_ICD_STATUS_DATE_KEY
		, CLAIM_ICD_STATUS_DETAIL_HKEY                       as                       CLAIM_ICD_STATUS_DETAIL_HKEY
		, CLM_ICD_STS_EFF_DT                                 as                       CLAIM_ICD_EFFECTIVE_DATE_KEY
		, CLM_ICD_STS_END_DT                                 as                      CLAIM_ICD_EXPIRATION_DATE_KEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_HKEY
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
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, CURRENT_ICD_IND                                    as                                    CURRENT_ICD_IND
		, CS_CLS_CD                                          as                                          CS_CLS_CD
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE 
        , VOID_IND                                           as                                           VOID_IND
        , CLM_ICD_STS_DT                                     as                                     CLM_ICD_STS_DT
				FROM     LOGIC_CLM   ), 
RENAME_ICD as ( SELECT 
		  ICD_HKEY                                           as                                           ICD_HKEY 
		, ICD_CODE                                           as                                           ICD_CODE
        , RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                                    as                                    RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
				FROM     LOGIC_ICD   ), 
RENAME_MC as ( SELECT 
		  MANUAL_CLASS_HKEY                                  as                             MANUAL_CLASS_CODE_HKEY 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
        , MANUAL_CLASS_CODE                                  as                                  MANUAL_CLASS_CODE
        , MANUAL_CLASS_SUFFIX_CODE                           as                           MANUAL_CLASS_SUFFIX_CODE
		, RECORD_EFFECTIVE_DATE                              as                           MC_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                 MC_RECORD_END_DATE
				FROM     LOGIC_MC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM 
                                            WHERE VOID_IND = 'N'  ),
FILTER_ICD                            as ( SELECT * from    RENAME_ICD   ),
FILTER_MC                             as ( SELECT * from    RENAME_MC   ),

---- JOIN LAYER ----

CLM as ( SELECT * 
				FROM  FILTER_CLM
				LEFT JOIN FILTER_ICD ON  coalesce( FILTER_CLM.ICD_CD,  'UNK') =  FILTER_ICD.ICD_CODE AND FILTER_ICD.CURRENT_RECORD_IND ='Y' 
				LEFT JOIN FILTER_MC  ON  coalesce( FILTER_CLM.CS_CLS_CD, '99999') =  FILTER_MC.MANUAL_CLASS_CODE 
                         AND coalesce(FILTER_CLM.DRVD_MANUAL_CLASS_SUFFIX_CODE, '99999') = FILTER_MC.MANUAL_CLASS_SUFFIX_CODE  
						 AND FILTER_CLM.CLM_OCCR_DATE BETWEEN  FILTER_MC.MC_RECORD_EFFECTIVE_DATE AND COALESCE(FILTER_MC.MC_RECORD_END_DATE, '2099-12-31') )
SELECT 
  HISTORY_ID
, CLAIM_NUMBER
, coalesce( CLAIM_ICD_SPECIFIC_DESC_HKEY, MD5( '99999' )) AS  CLAIM_ICD_SPECIFIC_DESC_HKEY
, CLAIM_ICD_STATUS_DETAIL_HKEY
, coalesce( ICD_HKEY, MD5( '-1111' )) AS  ICD_HKEY
, CLAIM_INITAL_FILE_DATE_KEY
, CLAIM_OCCURRENCE_DATE_KEY
, CLAIM_FILE_DATE_KEY
, CLAIM_ICD_STATUS_DATE_KEY
, CLAIM_ICD_EFFECTIVE_DATE_KEY
, CLAIM_ICD_EXPIRATION_DATE_KEY
, CLAIM_TYPE_STATUS_HKEY
, CLAIM_DETAIL_HKEY
, INDUSTRY_GROUP_HKEY
, coalesce( MANUAL_CLASS_CODE_HKEY, MD5( '-1111' )) AS  MANUAL_CLASS_CODE_HKEY
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
from CLM