

---- SRC LAYER ----
WITH
SRC_HAS            as ( SELECT *     FROM     STAGING.DST_HEALTHCARE_SERVICE_AUTHORIZATION ),
//SRC_HAS            as ( SELECT *     FROM     DST_HEALTHCARE_SERVICE_AUTHORIZATION) ,

---- LOGIC LAYER ----


LOGIC_HAS as ( SELECT 
		  CASE_SERV_ID                                       as                                       CASE_SERV_ID 
		, CASE_ID                                            as                                            CASE_ID 
		, CASE_NO                                            as                                            CASE_NO 
		, CASE_SERV_AUTH_NO                                  as                                  CASE_SERV_AUTH_NO 
		, CASE_SERV_AUTH_DT                                  as                                  CASE_SERV_AUTH_DT 
		, CASE_SERV_APRV_FR_DT                               as                               CASE_SERV_APRV_FR_DT 
		, CASE_SERV_APRV_TO_DT                               as                               CASE_SERV_APRV_TO_DT 
		, CASE_SERV_TYP_CD                                   as                                   CASE_SERV_TYP_CD 
		, CASE_SERV_STS_TYP_CD                               as                               CASE_SERV_STS_TYP_CD 
		, CSD_CD_FR                                          as                                          CSD_CD_FR 
		, CSD_CD_TO                                          as                                          CSD_CD_TO 
		, VOID_IND                                           as                                           VOID_IND 
		, CASE_CNTX_NO                                       as                                       CASE_CNTX_NO 
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD 
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD 
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD 
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD 
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD 
		, CTH_CLM_NO                                         as                                         CTH_CLM_NO 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
		, CTH_HIST_EFF_DT                                    as                                    CTH_HIST_EFF_DT 
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_END_DT 
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
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM 
        , OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM 
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD 
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM 
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM 
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM 
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD 
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM 
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1 
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2 
		, CLM_OCCR_LOC_COMT                                  as                                  CLM_OCCR_LOC_COMT 
		, CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD 
		, CASE_STT_STS_STT_EFF_DT                            as                            CASE_STT_STS_STT_EFF_DT 
		, CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD 
		, CASE_STT_STS_STS_EFF_DT                            as                            CASE_STT_STS_STS_EFF_DT 
		, CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD 
		, CASE_STT_STS_STS_RSN_EFF_DT                        as                        CASE_STT_STS_STS_RSN_EFF_DT 
		, CASE_HIST_HIST_EFF_DTM                             as                             CASE_HIST_HIST_EFF_DTM 
		, CASE_HIST_HIST_END_DTM                             as                             CASE_HIST_HIST_END_DTM 
		, CASE_INT_DT                                        as                                        CASE_INT_DT 
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT 
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT 
		, CASE_COMP_DT                                       as                                       CASE_COMP_DT 
		FROM SRC_HAS
            )

---- RENAME LAYER ----
,

RENAME_HAS        as ( SELECT 
		  CASE_SERV_ID                                       as                                       CASE_SERV_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_NO                                            as                                        CASE_NUMBER
		, CASE_SERV_AUTH_NO                                  as                  SERVICE_TYPE_AUTHORIZATION_NUMBER
		, CASE_SERV_AUTH_DT                                  as                                 AUTHORIZATION_DATE
		, CASE_SERV_APRV_FR_DT                               as                                 AUTHORIZATION_FROM
		, CASE_SERV_APRV_TO_DT                               as                              AUTHORIZATION_THROUGH
		, CASE_SERV_TYP_CD                                   as                    AUTHORIZATION_SERVICE_TYPE_CODE
		, CASE_SERV_STS_TYP_CD                               as                          AUTHORIZATION_STATUS_CODE
		, CSD_CD_FR                                          as                    AUTHORIZATION_SERVICE_CODE_FROM
		, CSD_CD_TO                                          as                 AUTHORIZATION_SERVICE_CODE_THROUGH
		, VOID_IND                                           as                                           VOID_IND
		, CASE_CNTX_NO                                       as                                       CLAIM_NUMBER
		, CASE_TYP_CD                                        as                                     CASE_TYPE_CODE
		, CASE_CTG_TYP_CD                                    as                                 CASE_CATEGORY_CODE
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD
		, CTH_CLM_NO                                         as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CTH_HIST_EFF_DT                                    as                                    CTH_HIST_EFF_DT
		, CTH_HIST_EFF_DT                                    as                                    CTH_HIST_END_DT
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
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM
        , OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2
		, CLM_OCCR_LOC_COMT                                  as                                  CLM_OCCR_LOC_COMT
		, CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD
		, CASE_STT_STS_STT_EFF_DT                            as                            CASE_STT_STS_STT_EFF_DT
		, CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD
		, CASE_STT_STS_STS_EFF_DT                            as                            CASE_STT_STS_STS_EFF_DT
		, CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD
		, CASE_STT_STS_STS_RSN_EFF_DT                        as                        CASE_STT_STS_STS_RSN_EFF_DT
		, CASE_HIST_HIST_EFF_DTM                             as                             CASE_HIST_HIST_EFF_DTM
		, CASE_HIST_HIST_END_DTM                             as                             CASE_HIST_HIST_END_DTM
		, CASE_INT_DT                                        as                               CASE_INITIATION_DATE
		, CASE_EFF_DT                                        as                                CASE_EFFECTIVE_DATE
		, CASE_DUE_DT                                        as                                      CASE_DUE_DATE
		, CASE_COMP_DT                                       as                               CASE_COMPLETION_DATE
				FROM     LOGIC_HAS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HAS                            as ( SELECT * FROM    RENAME_HAS   ),

---- JOIN LAYER ----

 JOIN_HAS         as  ( SELECT * 
				FROM  FILTER_HAS )
 SELECT * FROM  JOIN_HAS