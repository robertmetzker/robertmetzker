
  create or replace  view DEV_EDW.STAGING.DSV_CLAIM_ICD_STATUS_HISTORY  as (
    

---- SRC LAYER ----
WITH
SRC_ICD            as ( SELECT *     FROM     STAGING.DST_CLAIM_ICD_STATUS_HISTORY ),
//SRC_ICD            as ( SELECT *     FROM     DST_CLAIM_ICD_STATUS_HISTORY) ,

---- LOGIC LAYER ----

LOGIC_ICD as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, HIST_ID                                            as                                            HIST_ID 
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID 
		, AGRE_ID                                            as                                            AGRE_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, ICD_ID                                             as                                             ICD_ID 
		, ICD_CD                                             as                                             ICD_CD 
		, ICD_VER_CD                                         as                                         ICD_VER_CD 
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD 
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM 
		, CURRENT_ICD_IND                                    as                                    CURRENT_ICD_IND 
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND 
		, CLM_ICD_STS_EFF_DT                                 as                                 CLM_ICD_STS_EFF_DT 
		, CLM_ICD_STS_END_DT                                 as                                 CLM_ICD_STS_END_DT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		, VOID_IND                                           as                                           VOID_IND 
		, ICD_STS_DT                                         as                                         ICD_STS_DT 
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD 
		, ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM 
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD 
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM 
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC 
		, CTH_CLM_NO                                         as                                         CTH_CLM_NO 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
		, CTH_HIST_EFF_DT                                    as                                    CTH_HIST_EFF_DT 
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_END_DT 
		, CTH_CLM_REL_SNPSHT_IND                             as                             CTH_CLM_REL_SNPSHT_IND 
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
		, CSH_CLM_NO                                         as                                         CSH_CLM_NO 
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, CSH_HIST_EFF_DT                                    as                                    CSH_HIST_EFF_DT 
		, CSH_HIST_END_DT                                    as                                    CSH_HIST_END_DT 
		, CLM_CLM_NO                                         as                                         CLM_CLM_NO 
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
		, CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE 
		, CLAIM_FILE_DATE                                    as                                    CLAIM_FILE_DATE 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE 
		, CS_CLS_CD                                          as                                          CS_CLS_CD 
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE 
		FROM SRC_ICD
            )

---- RENAME LAYER ----
,

RENAME_ICD        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, HIST_ID                                            as                                            HIST_ID
		, CLM_ICD_STS_ID                                     as                                     CLM_ICD_STS_ID
		, AGRE_ID                                            as                                            AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, ICD_ID                                             as                                             ICD_ID
		, ICD_CD                                             as                                             ICD_CD
		, ICD_VER_CD                                         as                                         ICD_VER_CD
		, ICD_STS_TYP_CD                                     as                                     ICD_STS_TYP_CD
		, ICD_STS_TYP_NM                                     as                                     ICD_STS_TYP_NM
		, CURRENT_ICD_IND                                    as                                     CURRENT_ICD_IND
		, CLM_ICD_STS_PRI_IND                                as                                CLM_ICD_STS_PRI_IND
		, CLM_ICD_STS_EFF_DT                                 as                                 CLM_ICD_STS_EFF_DT
		, CLM_ICD_STS_END_DT                                 as                                 CLM_ICD_STS_END_DT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT
		, HIST_END_DT                                        as                                        HIST_END_DT
		, VOID_IND                                           as                                           VOID_IND
		, ICD_STS_DT                                         as                                         ICD_STS_DT
		, ICD_LOC_TYP_CD                                     as                                     ICD_LOC_TYP_CD
		, ICD_LOC_TYP_NM                                     as                                     ICD_LOC_TYP_NM
		, ICD_SITE_TYP_CD                                    as                                    ICD_SITE_TYP_CD
		, ICD_SITE_TYP_NM                                    as                                    ICD_SITE_TYP_NM
		, CLM_ICD_DESC                                       as                                       CLM_ICD_DESC
		, CTH_CLM_NO                                         as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CTH_HIST_EFF_DT                                    as                                    CTH_HIST_EFF_DT
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_END_DT
		, CTH_CLM_REL_SNPSHT_IND                             as                             CTH_CLM_REL_SNPSHT_IND
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND
		, CSH_CLM_NO                                         as                                         CSH_CLM_NO
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, CSH_HIST_EFF_DT                                    as                                    CSH_HIST_EFF_DT
		, CSH_HIST_END_DT                                    as                                    CSH_HIST_END_DT
		, CLM_CLM_NO                                         as                                         CLM_CLM_NO
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
		, CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE
		, CLAIM_FILE_DATE                                    as                                    CLAIM_FILE_DATE
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE
		, CS_CLS_CD                                          as                                          CS_CLS_CD
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE 
				FROM     LOGIC_ICD   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ICD                            as ( SELECT * FROM    RENAME_ICD   ),

---- JOIN LAYER ----

 JOIN_ICD         as  ( SELECT * 
				FROM  FILTER_ICD )
 SELECT * FROM  JOIN_ICD
  );
