

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_ICD_STATUS_HISTORY  as
      (---- SRC LAYER ----
WITH
SRC_ICD            as ( SELECT *     FROM     STAGING.STG_CLAIM_ICD_STATUS_HISTORY ),
SRC_CTH            as ( SELECT *     FROM     STAGING.DST_CLAIM_TYPE_HISTORY ),
SRC_CSH            as ( SELECT *     FROM     STAGING.DST_CLAIM_STATUS_HISTORY ),
SRC_CLM            as ( SELECT *     FROM     STAGING.DST_CLAIM ),
//SRC_ICD            as ( SELECT *     FROM     STG_CLAIM_ICD_STATUS_HISTORY) ,
//SRC_CTH            as ( SELECT *     FROM     DST_CLAIM_TYPE_HISTORY) ,
//SRC_CSH            as ( SELECT *     FROM     DST_CLAIM_STATUS_HISTORY) ,
//SRC_CLM            as ( SELECT *     FROM     DST_CLAIM) ,

---- LOGIC LAYER ----


LOGIC_ICD as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID 
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, ICD_ID                                             as                                             ICD_ID 
		, TRIM( ICD_CD )                                     as                                             ICD_CD 
		, TRIM( ICD_VER_CD )                                 as                                         ICD_VER_CD 
		, TRIM( ICD_STS_TYP_CD )                             as                                     ICD_STS_TYP_CD 
		, TRIM( ICD_STS_TYP_NM )                             as                                     ICD_STS_TYP_NM 
		, TRIM( CLM_ICD_STS_PRI_IND )                        as                                CLM_ICD_STS_PRI_IND 
		, CLM_ICD_STS_EFF_DT                                 as                                 CLM_ICD_STS_EFF_DT 
		, CLM_ICD_STS_END_DT                                 as                                 CLM_ICD_STS_END_DT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, cast( AUDIT_USER_UPDT_DTM as DATE )                as                                AUDIT_USER_UPDT_DTM 
		, cast( HIST_EFF_DTM as DATE )                       as                                       HIST_EFF_DTM 
		, cast( HIST_END_DTM as DATE )                       as                                       HIST_END_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, ICD_STS_DT                                         as                                         ICD_STS_DT 
		, TRIM( ICD_LOC_TYP_CD )                             as                                     ICD_LOC_TYP_CD 
		, TRIM( ICD_LOC_TYP_NM )                             as                                     ICD_LOC_TYP_NM 
		, TRIM( ICD_SITE_TYP_CD )                            as                                    ICD_SITE_TYP_CD 
		, TRIM( ICD_SITE_TYP_NM )                            as                                    ICD_SITE_TYP_NM 
		, TRIM( CLM_ICD_DESC )                               as                                       CLM_ICD_DESC 
		FROM SRC_ICD
            ),

LOGIC_CTH as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( CLM_TYP_NM )                                 as                                         CLM_TYP_NM 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, TRIM( CHNG_OVR_IND )                               as                                       CHNG_OVR_IND 
		FROM SRC_CTH
            ),

LOGIC_CSH as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, TRIM( CLM_STS_TYP_CD )                             as                                     CLM_STS_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		FROM SRC_CSH
            ),

LOGIC_CLM as ( SELECT 
		  CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE 
		, CLAIM_FILE_DATE                                    as                                    CLAIM_FILE_DATE 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE 
		, TRIM( CS_CLS_CD )                                  as                                          CS_CLS_CD 
		, TRIM( DRVD_MANUAL_CLASS_SUFFIX_CODE )              as                      DRVD_MANUAL_CLASS_SUFFIX_CODE   
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( OCCR_SRC_TYP_NM )                            as                                    OCCR_SRC_TYP_NM 
		, TRIM( OCCR_MEDA_TYP_NM )                           as                                   OCCR_MEDA_TYP_NM 
		, TRIM( NOI_CTG_TYP_NM )                             as                                     NOI_CTG_TYP_NM 
		, TRIM( NOI_TYP_NM )                                 as                                         NOI_TYP_NM 
		, TRIM( FIREFIGHTER_CANCER_IND )                     as                             FIREFIGHTER_CANCER_IND 
		, TRIM( COVID_EXPOSURE_IND )                         as                                 COVID_EXPOSURE_IND 
		, TRIM( COVID_EMERGENCY_WORKER_IND )                 as                         COVID_EMERGENCY_WORKER_IND 
		, TRIM( COVID_HEALTH_CARE_WORKER_IND )               as                       COVID_HEALTH_CARE_WORKER_IND 
		, TRIM( COMBINED_CLAIM_IND )                         as                                 COMBINED_CLAIM_IND 
		, TRIM( SB223_IND )                                  as                                          SB223_IND 
		, TRIM( OCCR_PRMS_TYP_NM )                           as                                   OCCR_PRMS_TYP_NM 
		, TRIM( EMPLOYER_PREMISES_IND )                      as                              EMPLOYER_PREMISES_IND 
		, TRIM( CLM_CTRPH_INJR_IND )                         as                                 CLM_CTRPH_INJR_IND 
		, TRIM( K_PROGRAM_ENROLLMENT_DESC )                  as                          K_PROGRAM_ENROLLMENT_DESC 
		, TRIM( K_PROGRAM_TYPE_DESC )                        as                                K_PROGRAM_TYPE_DESC 
		, K_PROGRAM_START_DATE                               as                               K_PROGRAM_START_DATE 
		, K_PROGRAM_END_DATE                                 as                                 K_PROGRAM_END_DATE 
		, TRIM( K_PROGRAM_REASON_CODE )                      as                              K_PROGRAM_REASON_CODE 
		, TRIM( K_PROGRAM_REASON_DESC )                      as                              K_PROGRAM_REASON_DESC 
		, TRIM( CUST_NO )                                    as                                            CUST_NO 
		, TRIM( EMP_CUST_NO )                                as                                        EMP_CUST_NO 
		, TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, TRIM( POLICY_TYPE_CODE )                           as                                   POLICY_TYPE_CODE 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD 
		, TRIM( POLICY_ACTIVE_IND )                          as                                  POLICY_ACTIVE_IND
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
		FROM SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_ICD        as ( SELECT 
		  HIST_ID                                            as                                            HIST_ID
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, ICD_ID                                             as                                             ICD_ID
		, ICD_CD                                             as                                             ICD_CD
		, ICD_VER_CD                                         as                                         ICD_VER_CD
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, CLM_ICD_STS_EFF_DT                                 as                                 CLM_ICD_STS_EFF_DT
		, CLM_ICD_STS_END_DT                                 as                                 CLM_ICD_STS_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DTM                                       as                                        HIST_EFF_DT
		, HIST_END_DTM                                       as                                        HIST_END_DT
		, VOID_IND                                           as                                           VOID_IND
		, ICD_STS_DT                                         as                                         ICD_STS_DT
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC 
				FROM     LOGIC_ICD   ), 
RENAME_CTH        as ( SELECT 
		  CLM_NO                                             as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, HIST_EFF_DT                                        as                                    CTH_HIST_EFF_DT
		, HIST_END_DT                                        as                                    CTH_HIST_END_DT
		, CLM_REL_SNPSHT_IND                                 as                             CTH_CLM_REL_SNPSHT_IND
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
				FROM     LOGIC_CTH   ), 
RENAME_CSH        as ( SELECT 
		  CLM_NO                                             as                                         CSH_CLM_NO
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, HIST_EFF_DT                                        as                                    CSH_HIST_EFF_DT
		, HIST_END_DT                                        as                                    CSH_HIST_END_DT 
				FROM     LOGIC_CSH   ), 
RENAME_CLM        as ( SELECT 
		  CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE
		, CLAIM_FILE_DATE                                    as                                    CLAIM_FILE_DATE
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE
		, CS_CLS_CD                                          as                                          CS_CLS_CD
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE
		, CLM_NO                                             as                                         CLM_CLM_NO
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
		, CUST_NO                                            as                                            CUST_NO
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO
		, PLCY_NO                                            as                                            PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, CLM_REL_SNPSHT_IND                                 as                             CLM_CLM_REL_SNPSHT_IND                          
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * FROM    RENAME_ICD   
                                           WHERE CLM_REL_SNPSHT_IND = 'N'),
FILTER_CTH                            as ( SELECT * FROM    RENAME_CTH   ),
FILTER_CSH                            as ( SELECT * FROM    RENAME_CSH   ),
FILTER_CLM                            as ( SELECT * FROM    RENAME_CLM  ),

---- JOIN LAYER ----

ICD as ( SELECT * 
				FROM  FILTER_ICD
				LEFT JOIN FILTER_CTH ON  FILTER_ICD.CLM_NO =  FILTER_CTH.CTH_CLM_NO AND HIST_EFF_DT BETWEEN CTH_HIST_EFF_DT AND coalesce(CTH_HIST_END_DT, '2099-12-31') 
								LEFT JOIN FILTER_CSH ON  FILTER_ICD.CLM_NO =  FILTER_CSH.CSH_CLM_NO AND HIST_EFF_DT BETWEEN CSH_HIST_EFF_DT  AND coalesce(CSH_HIST_END_DT, '2099-12-31') 
								LEFT JOIN FILTER_CLM ON  FILTER_ICD.CLM_NO =  FILTER_CLM.CLM_CLM_NO ),

------ETL LAYER------------
ETL AS(SELECT 
md5(cast(
    
    coalesce(cast(HIST_ID as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,HIST_ID
,CLM_ICD_STS_ID
,AGRE_ID
,CLM_NO
,CLM_REL_SNPSHT_IND
,ICD_ID
,ICD_CD
,ICD_VER_CD
,ICD_STS_TYP_CD
,ICD_STS_TYP_NM
,CASE WHEN HIST_END_DT IS NULL THEN 'Y' ELSE 'N' END AS CURRENT_ICD_IND
,CLM_ICD_STS_PRI_IND
,CLM_ICD_STS_EFF_DT
,CLM_ICD_STS_END_DT
,AUDIT_USER_ID_CREA
,AUDIT_USER_CREA_DTM
,AUDIT_USER_ID_UPDT
,AUDIT_USER_UPDT_DTM
,HIST_EFF_DT
,HIST_END_DT
,VOID_IND
,ICD_STS_DT
,ICD_LOC_TYP_CD
,CASE WHEN ICD_LOC_TYP_NM = '' THEN NULL ELSE ICD_LOC_TYP_NM END AS ICD_LOC_TYP_NM
,ICD_SITE_TYP_CD
,CASE WHEN ICD_SITE_TYP_NM = ''  THEN NULL ELSE ICD_SITE_TYP_NM END AS ICD_SITE_TYP_NM
,CLM_ICD_DESC
,CTH_CLM_NO
,CLM_TYP_CD
,CLM_TYP_NM
,CTH_HIST_EFF_DT
,CTH_HIST_END_DT
,CTH_CLM_REL_SNPSHT_IND
,CLM_CLM_REL_SNPSHT_IND
,CHNG_OVR_IND
,CSH_CLM_NO
,CLM_STT_TYP_CD
,CLM_STS_TYP_CD
,CLM_TRANS_RSN_TYP_CD
,CSH_HIST_EFF_DT
,CSH_HIST_END_DT
,CLAIM_INITIAL_FILE_DATE
,CLAIM_FILE_DATE
,CLM_OCCR_DATE
,INDUSTRY_GROUP_CODE
,CS_CLS_CD
,DRVD_MANUAL_CLASS_SUFFIX_CODE
,CLM_CLM_NO
,OCCR_SRC_TYP_NM
,OCCR_MEDA_TYP_NM
,NOI_CTG_TYP_NM
,NOI_TYP_NM
,FIREFIGHTER_CANCER_IND
,COVID_EXPOSURE_IND
,COVID_EMERGENCY_WORKER_IND
,COVID_HEALTH_CARE_WORKER_IND
,COMBINED_CLAIM_IND
,SB223_IND
,OCCR_PRMS_TYP_NM
,EMPLOYER_PREMISES_IND
,CLM_CTRPH_INJR_IND
,K_PROGRAM_ENROLLMENT_DESC
,K_PROGRAM_TYPE_DESC
,K_PROGRAM_START_DATE
,K_PROGRAM_END_DATE
,K_PROGRAM_REASON_CODE
,K_PROGRAM_REASON_DESC
,CUST_NO
,EMP_CUST_NO
,PLCY_NO
,BUSN_SEQ_NO
,POLICY_TYPE_CODE
,PLCY_STS_TYP_CD
,PLCY_STS_RSN_TYP_CD
,POLICY_ACTIVE_IND
FROM ICD)

SELECT * FROM ETL
      );
    