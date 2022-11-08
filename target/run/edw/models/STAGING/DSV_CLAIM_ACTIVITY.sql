
  create or replace  view DEV_EDW_32600145.STAGING.DSV_CLAIM_ACTIVITY  as (
    

---- SRC LAYER ----
WITH
SRC_AC             as ( SELECT *     FROM     STAGING.DST_CLAIM_ACTIVITY ),
//SRC_AC             as ( SELECT *     FROM     DST_CLAIM_ACTIVITY) ,

---- LOGIC LAYER ----

LOGIC_AC as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, ACTV_ID                                            as                                            ACTV_ID 
		, ACTV_DTL_ID                                        as                                        ACTV_DTL_ID 
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID 
		, CLM_NO                                             as                                             CLM_NO 
		, CNTX_TYP_NM                                        as                                        CNTX_TYP_NM 
		, SUBLOC_TYP_NM                                      as                                      SUBLOC_TYP_NM 
		, ACTV_NM_TYP_NM                                     as                                     ACTV_NM_TYP_NM 
		, ACTV_ACTN_TYP_NM                                   as                                   ACTV_ACTN_TYP_NM 
		, ACTV_DTL_DESC                                      as                                      ACTV_DTL_DESC 
		, ACTV_DTL_COL_NM                                    as                                    ACTV_DTL_COL_NM 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, USER_LGN_NM                                        as                                        USER_LGN_NM 
		, USER_DRV_UPCS_NM                                   as                                   USER_DRV_UPCS_NM 
		, SUPERVISOR_DRV_UPCS_NM                             as                             SUPERVISOR_DRV_UPCS_NM 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
		, FNCT_ROLE_NM                                       as                                       FNCT_ROLE_NM 
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
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND 
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC 
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC 
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM 
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, PLCY_NO                                            as                                            PLCY_NO 
		, CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE 
		FROM SRC_AC
            )

---- RENAME LAYER ----
,

RENAME_AC         as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, ACTV_ID                                            as                                        ACTIVITY_ID
		, ACTV_DTL_ID                                        as                                 ACTIVITY_DETAIL_ID
		, CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, CNTX_TYP_NM                                        as                         ACTIVITY_CONTEXT_TYPE_NAME
		, SUBLOC_TYP_NM                                      as                                       PROCESS_AREA
		, ACTV_NM_TYP_NM                                     as                                 ACTIVITY_NAME_TYPE
		, ACTV_ACTN_TYP_NM                                   as                                        ACTION_TYPE
		, ACTV_DTL_DESC                                      as                               ACTIVITY_DETAIL_DESC
		, ACTV_DTL_COL_NM                                    as                      ACTIVITY_SUBCONTEXT_TYPE_NAME
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, USER_LGN_NM                                        as                                        USER_LGN_NM
		, USER_DRV_UPCS_NM                                   as                                   USER_DRV_UPCS_NM
		, SUPERVISOR_DRV_UPCS_NM                             as                             SUPERVISOR_DRV_UPCS_NM
		, CLM_TYP_CD                                         as                  CURRENT_CORESUITE_CLAIM_TYPE_CODE
		, CLM_STT_TYP_CD                                     as                                   CLAIM_STATE_CODE
		, CLM_STS_TYP_CD                                     as                                  CLAIM_STATUS_CODE
		, CLM_TRANS_RSN_TYP_CD                               as                           CLAIM_STATUS_REASON_CODE
		, CHNG_OVR_IND                                       as                            CLAIM_TYPE_CHNG_OVR_IND
		, FNCT_ROLE_NM                                       as                          USER_FUNCTIONAL_ROLE_DESC 
		, OCCR_SRC_TYP_NM                                    as                                 FILING_SOURCE_DESC
		, OCCR_MEDA_TYP_NM                                   as                                  FILING_MEDIA_DESC
		, NOI_CTG_TYP_NM                                     as                          NATURE_OF_INJURY_CATEGORY
		, NOI_TYP_NM                                         as                              NATURE_OF_INJURY_TYPE
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND
		, COMBINED_CLAIM_IND                                 as                                       COMBINED_IND
		, SB223_IND                                          as                                          SB223_IND
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC
		, CLM_CTRPH_INJR_IND                                 as                                   CATASTROPHIC_IND
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE 
				FROM     LOGIC_AC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_AC                             as ( SELECT * FROM    RENAME_AC   ),

---- JOIN LAYER ----

 JOIN_AC          as  ( SELECT * 
				FROM  FILTER_AC )
 SELECT * FROM  JOIN_AC
  );
