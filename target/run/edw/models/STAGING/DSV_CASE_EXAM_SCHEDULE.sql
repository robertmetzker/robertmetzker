
  create or replace  view DEV_EDW.STAGING.DSV_CASE_EXAM_SCHEDULE  as (
    

---- SRC LAYER ----
WITH
SRC_CES            as ( SELECT *     FROM     STAGING.DST_CASE_EXAM_SCHEDULE ),
//SRC_CES            as ( SELECT *     FROM     DST_CASE_EXAM_SCHEDULE) ,

---- LOGIC LAYER ----


LOGIC_CES as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID 
		, CASE_NO                                            as                                            CASE_NO 
		, CASE_CNTX_ID                                       as                                       CASE_CNTX_ID 
		, CASE_CNTX_NO                                       as                                       CASE_CNTX_NO 
		, CASE_NM                                            as                                            CASE_NM 
		, CASE_EXTRNL_NO                                     as                                     CASE_EXTRNL_NO 
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD 
		, APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM 
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD 
		, CASE_CTG_TYP_NM                                    as                                    CASE_CTG_TYP_NM 
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD 
		, CASE_TYP_NM                                        as                                        CASE_TYP_NM 
		, CASE_INT_DT                                        as                                        CASE_INT_DT 
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT 
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT 
		, CASE_COMP_DT                                       as                                       CASE_COMP_DT 
		, CASE_RSN_SUM_TXT                                   as                                   CASE_RSN_SUM_TXT 
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD 
		, CASE_PRTY_TYP_NM                                   as                                   CASE_PRTY_TYP_NM 
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD 
		, CASE_RSOL_TYP_NM                                   as                                   CASE_RSOL_TYP_NM 
		, CASE_SRC_TYP_CD                                    as                                    CASE_SRC_TYP_CD 
		, CASE_SRC_TYP_NM                                    as                                    CASE_SRC_TYP_NM 
		, CASE_ACTN_PLN_TXT                                  as                                  CASE_ACTN_PLN_TXT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, VOID_IND                                           as                                           VOID_IND 
		, CDES_ID                                            as                                            CDES_ID 
		, EXAM_CASE_ID                                       as                                       EXAM_CASE_ID 
		, CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD 
		, CD_EXM_REQS_TYP_NM                                 as                                 CD_EXM_REQS_TYP_NM 
		, CPS_TYP_CD                                         as                                         CPS_TYP_CD 
		, CPS_TYP_NM                                         as                                         CPS_TYP_NM 
		, CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND 
		, CPS_TYP_NM_SCND                                    as                                    CPS_TYP_NM_SCND 
		, LANG_TYP_CD                                        as                                        LANG_TYP_CD 
		, LANG_TYP_NM                                        as                                        LANG_TYP_NM 
		, CDES_EXM_DT                                        as                                        CDES_EXM_DT 
		, CDES_EXM_RPT_RECV_DT                               as                               CDES_EXM_RPT_RECV_DT 
		, CDES_EXM_PHYS_IMPR_RT                              as                              CDES_EXM_PHYS_IMPR_RT 
		, CDES_EXM_FNL_IMPR_RT                               as                               CDES_EXM_FNL_IMPR_RT 
		, CDES_CLMT_AVL_NAR_DESC                             as                             CDES_CLMT_AVL_NAR_DESC 
		, CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND 
		, CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND 
		, CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND 
		, CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND 
		, CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND 
		, CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND 
		, CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND 
		, CDES_SPL_REQD                                      as                                      CDES_SPL_REQD 
		, CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND 
		, CDES_EXM_ADDR_STR_1                                as                                CDES_EXM_ADDR_STR_1 
		, CDES_EXM_ADDR_STR_2                                as                                CDES_EXM_ADDR_STR_2 
		, CDES_EXM_ADDR_CITY_NM                              as                              CDES_EXM_ADDR_CITY_NM 
		, CDES_EXM_ADDR_CNTY_NM                              as                              CDES_EXM_ADDR_CNTY_NM 
		, CDES_EXM_ADDR_POST_CD                              as                              CDES_EXM_ADDR_POST_CD 
		, CDES_EXM_ADDR_STT_ABRV                             as                             CDES_EXM_ADDR_STT_ABRV 
		, CDES_EXM_ADDR_STT_NM                               as                               CDES_EXM_ADDR_STT_NM 
		, CDES_EXM_ADDR_CNTRY_NM                             as                             CDES_EXM_ADDR_CNTRY_NM 
		, CDES_EXM_UN_NRML_SITU                              as                              CDES_EXM_UN_NRML_SITU 
		, CDES_EXM_QA                                        as                                        CDES_EXM_QA 
		, CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND 
		, CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND 
		, CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND 
		, CDES_REQS_ADDTNL_TST                               as                               CDES_REQS_ADDTNL_TST 
		, CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND 
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD 
		, CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM 
		, CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND 
		, CDES_ADR_NO                                        as                                        CDES_ADR_NO 
		, CDES_ADR_TYP                                       as                                       CDES_ADR_TYP 
		, CDES_ADR_TRT_REQSTR                                as                                CDES_ADR_TRT_REQSTR 
		, CDES_ADR_TRT_DSP                                   as                                   CDES_ADR_TRT_DSP 
		, EXAM_VOID_IND                                      as                                      EXAM_VOID_IND 
		, CTH_CLM_NO                                         as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_EFF_DT
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_END_DT
		, CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
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
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
		FROM SRC_CES
            )

---- RENAME LAYER ----
,

RENAME_CES        as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID
		, CASE_NO                                            as                                        CASE_NUMBER
		, CASE_CNTX_ID                                       as                                       CASE_AGRE_ID
		, CASE_CNTX_NO                                       as                                  CASE_CLAIM_NUMBER
		, CASE_NM                                            as                                          CASE_NAME
		, CASE_EXTRNL_NO                                     as                               CASE_EXTERNAL_NUMBER
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, APP_CNTX_TYP_NM                                    as                                    APP_CNTX_TYP_NM
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD
		, CASE_CTG_TYP_NM                                    as                                    CASE_CTG_TYP_NM
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD
		, CASE_TYP_NM                                        as                                        CASE_TYP_NM
		, CASE_INT_DT                                        as                                        CASE_INT_DT
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT
		, CASE_COMP_DT                                       as                                 CASE_COMPLETE_DATE
		, CASE_RSN_SUM_TXT                                   as                                   CASE_RSN_SUM_TXT
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD
		, CASE_PRTY_TYP_NM                                   as                                   CASE_PRTY_TYP_NM
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD
		, CASE_RSOL_TYP_NM                                   as                                   CASE_RSOL_TYP_NM
		, CASE_SRC_TYP_CD                                    as                                    CASE_SRC_TYP_CD
		, CASE_SRC_TYP_NM                                    as                                    CASE_SRC_TYP_NM
		, CASE_ACTN_PLN_TXT                                  as                                  CASE_ACTN_PLN_TXT
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
		, VOID_IND                                           as                                           VOID_IND
		, CDES_ID                                            as                                            CDES_ID
		, EXAM_CASE_ID                                       as                                       EXAM_CASE_ID
		, CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD
		, CD_EXM_REQS_TYP_NM                                 as                                 CD_EXM_REQS_TYP_NM
		, CPS_TYP_CD                                         as                                         CPS_TYP_CD
		, CPS_TYP_NM                                         as                                         CPS_TYP_NM
		, CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND
		, CPS_TYP_NM_SCND                                    as                                    CPS_TYP_NM_SCND
		, LANG_TYP_CD                                        as                                        LANG_TYP_CD
		, LANG_TYP_NM                                        as                                        LANG_TYP_NM
		, CDES_EXM_DT                                        as                                        CDES_EXM_DT
		, CDES_EXM_RPT_RECV_DT                               as                               CDES_EXM_RPT_RECV_DT
		, CDES_EXM_PHYS_IMPR_RT                              as                              CDES_EXM_PHYS_IMPR_RT
		, CDES_EXM_FNL_IMPR_RT                               as                               CDES_EXM_FNL_IMPR_RT
		, CDES_CLMT_AVL_NAR_DESC                             as                             CDES_CLMT_AVL_NAR_DESC
		, CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND
		, CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND
		, CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND
		, CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND
		, CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND
		, CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND
		, CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND
		, CDES_SPL_REQD                                      as                                      CDES_SPL_REQD
		, CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND
		, CDES_EXM_ADDR_STR_1                                as                                CDES_EXM_ADDR_STR_1
		, CDES_EXM_ADDR_STR_2                                as                                CDES_EXM_ADDR_STR_2
		, CDES_EXM_ADDR_CITY_NM                              as                              CDES_EXM_ADDR_CITY_NM
		, CDES_EXM_ADDR_CNTY_NM                              as                              CDES_EXM_ADDR_CNTY_NM
		, CDES_EXM_ADDR_POST_CD                              as                              CDES_EXM_ADDR_POST_CD
		, CDES_EXM_ADDR_STT_ABRV                             as                             CDES_EXM_ADDR_STT_ABRV
		, CDES_EXM_ADDR_STT_NM                               as                               CDES_EXM_ADDR_STT_NM
		, CDES_EXM_ADDR_CNTRY_NM                             as                             CDES_EXM_ADDR_CNTRY_NM
		, CDES_EXM_UN_NRML_SITU                              as                              CDES_EXM_UN_NRML_SITU
		, CDES_EXM_QA                                        as                                        CDES_EXM_QA
		, CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND
		, CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND
		, CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND
		, CDES_REQS_ADDTNL_TST                               as                               CDES_REQS_ADDTNL_TST
		, CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD
		, CD_ADNDM_REQS_TYP_NM                               as                               CD_ADNDM_REQS_TYP_NM
		, CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND
		, CDES_ADR_NO                                        as                                        CDES_ADR_NO
		, CDES_ADR_TYP                                       as                                       CDES_ADR_TYP
		, CDES_ADR_TRT_REQSTR                                as                                CDES_ADR_TRT_REQSTR
		, CDES_ADR_TRT_DSP                                   as                                   CDES_ADR_TRT_DSP
		, AUDIT_USER_ID_CREA                                 as                            EXAM_AUDIT_USER_ID_CREA
		, AUDIT_USER_CREA_DTM                                as                           EXAM_AUDIT_USER_CREA_DTM
		, AUDIT_USER_ID_UPDT                                 as                            EXAM_AUDIT_USER_ID_UPDT
		, AUDIT_USER_UPDT_DTM                                as                           EXAM_AUDIT_USER_UPDT_DTM
		, EXAM_VOID_IND                                      as                                      EXAM_VOID_IND
		, CTH_CLM_NO                                         as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_EFF_DT
		, CTH_HIST_END_DT                                    as                                    CTH_HIST_END_DT
		, CLM_REL_SNPSHT_IND                                 as                             CTH_CLM_REL_SNPSHT_IND
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
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
				FROM     LOGIC_CES   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CES                            as ( SELECT * FROM    RENAME_CES   ),

---- JOIN LAYER ----

 JOIN_CES         as  ( SELECT * 
				FROM  FILTER_CES )
 SELECT * FROM  JOIN_CES
  );
