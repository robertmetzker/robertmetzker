---- SRC LAYER ----
WITH
SRC_CASES          as ( SELECT *     FROM     STAGING.STG_CASES ),
SRC_EXAM           as ( SELECT *     FROM     STAGING.STG_CASE_DETAIL_EXAM_SCHEDULE ),
SRC_CTH            as ( SELECT *     FROM     STAGING.DST_CLAIM_TYPE_HISTORY ),
SRC_CSH            as ( SELECT *     FROM     STAGING.DST_CLAIM_STATUS_HISTORY ),
SRC_CLM            as ( SELECT *     FROM     STAGING.DST_CLAIM ),
//SRC_CASES          as ( SELECT *     FROM     STG_CASES) ,
//SRC_EXAM           as ( SELECT *     FROM     STG_CASE_DETAIL_EXAM_SCHEDULE) ,
//SRC_CTH            as ( SELECT *     FROM     DST_CLAIM_TYPE_HISTORY) ,
//SRC_CSH            as ( SELECT *     FROM     DST_CLAIM_STATUS_HISTORY) ,
//SRC_CLM            as ( SELECT *     FROM     DST_CLAIM) ,

---- LOGIC LAYER ----


LOGIC_CASES as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID 
		, TRIM( CASE_NO )                                    as                                            CASE_NO 
		, CASE_CNTX_ID                                       as                                       CASE_CNTX_ID 
		, TRIM( CASE_CNTX_NO )                               as                                       CASE_CNTX_NO 
		, TRIM( CASE_NM )                                    as                                            CASE_NM 
		, TRIM( CASE_EXTRNL_NO )                             as                                     CASE_EXTRNL_NO 
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, TRIM( APP_CNTX_TYP_NM )                            as                                    APP_CNTX_TYP_NM 
		, TRIM( CASE_CTG_TYP_CD )                            as                                    CASE_CTG_TYP_CD 
		, TRIM( CASE_CTG_TYP_NM )                            as                                    CASE_CTG_TYP_NM 
		, TRIM( CASE_TYP_CD )                                as                                        CASE_TYP_CD 
		, TRIM( CASE_TYP_NM )                                as                                        CASE_TYP_NM 
		, CASE_INT_DT                                        as                                        CASE_INT_DT 
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT 
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT 
		, CASE_COMP_DT                                       as                                       CASE_COMP_DT 
		, TRIM( CASE_RSN_SUM_TXT )                           as                                   CASE_RSN_SUM_TXT 
		, TRIM( CASE_PRTY_TYP_CD )                           as                                   CASE_PRTY_TYP_CD 
		, TRIM( CASE_PRTY_TYP_NM )                           as                                   CASE_PRTY_TYP_NM 
		, TRIM( CASE_RSOL_TYP_CD )                           as                                   CASE_RSOL_TYP_CD 
		, TRIM( CASE_RSOL_TYP_NM )                           as                                   CASE_RSOL_TYP_NM 
		, TRIM( CASE_SRC_TYP_CD )                            as                                    CASE_SRC_TYP_CD 
		, TRIM( CASE_SRC_TYP_NM )                            as                                    CASE_SRC_TYP_NM 
		, TRIM( CASE_ACTN_PLN_TXT )                          as                                  CASE_ACTN_PLN_TXT 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_CASES
            ),

LOGIC_EXAM as ( SELECT 
		  CDES_ID                                            as                                            CDES_ID 
		,  md5(cast(
    
    coalesce(cast(CD_EXM_REQS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CPS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CPS_TYP_CD_SCND as 
    varchar
), '') || '-' || coalesce(cast(CD_ADNDM_REQS_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                EXAM_CASE_DETAIL_ID 
		,  md5(cast(
    
    coalesce(cast(CDES_CLMT_AVL_MON_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_CLMT_AVL_TUE_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_CLMT_AVL_WED_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_CLMT_AVL_THU_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_CLMT_AVL_FRI_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_CLMT_AVL_SAT_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_CLMT_AVL_SUN_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_ITPRT_NEED_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_GRTT_45_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_TRVL_REMB_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_ADDTNL_TST_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_ADNDM_REQS_IND as 
    varchar
), '') || '-' || coalesce(cast(CDES_RSLT_SUSPD_IND as 
    varchar
), '')

 as 
    varchar
)) 
                                                             as                                   EXAM_SCHEDULE_ID 
		, CASE_ID                                            as                                            CASE_ID 
		, TRIM( CD_EXM_REQS_TYP_CD )                         as                                 CD_EXM_REQS_TYP_CD 
		, TRIM( CD_EXM_REQS_TYP_NM )                         as                                 CD_EXM_REQS_TYP_NM 
		, TRIM( CPS_TYP_CD )                                 as                                         CPS_TYP_CD 
		, TRIM( CPS_TYP_NM )                                 as                                         CPS_TYP_NM 
		, TRIM( CPS_TYP_CD_SCND )                            as                                    CPS_TYP_CD_SCND 
		, TRIM( CPS_TYP_NM_SCND )                            as                                    CPS_TYP_NM_SCND 
		, TRIM( LANG_TYP_CD )                                as                                        LANG_TYP_CD 
		, TRIM( LANG_TYP_NM )                                as                                        LANG_TYP_NM 
		, CDES_EXM_DT                                        as                                        CDES_EXM_DT 
		, CDES_EXM_RPT_RECV_DT                               as                               CDES_EXM_RPT_RECV_DT 
		, CDES_EXM_PHYS_IMPR_RT                              as                              CDES_EXM_PHYS_IMPR_RT 
		, CDES_EXM_FNL_IMPR_RT                               as                               CDES_EXM_FNL_IMPR_RT 
		, TRIM( CDES_CLMT_AVL_NAR_DESC )                     as                             CDES_CLMT_AVL_NAR_DESC 
		, TRIM( CDES_CLMT_AVL_MON_IND )                      as                              CDES_CLMT_AVL_MON_IND 
		, TRIM( CDES_CLMT_AVL_TUE_IND )                      as                              CDES_CLMT_AVL_TUE_IND 
		, TRIM( CDES_CLMT_AVL_WED_IND )                      as                              CDES_CLMT_AVL_WED_IND 
		, TRIM( CDES_CLMT_AVL_THU_IND )                      as                              CDES_CLMT_AVL_THU_IND 
		, TRIM( CDES_CLMT_AVL_FRI_IND )                      as                              CDES_CLMT_AVL_FRI_IND 
		, TRIM( CDES_CLMT_AVL_SAT_IND )                      as                              CDES_CLMT_AVL_SAT_IND 
		, TRIM( CDES_CLMT_AVL_SUN_IND )                      as                              CDES_CLMT_AVL_SUN_IND 
		, TRIM( CDES_SPL_REQD )                              as                                      CDES_SPL_REQD 
		, TRIM( CDES_ITPRT_NEED_IND )                        as                                CDES_ITPRT_NEED_IND 
		, TRIM( CDES_EXM_ADDR_STR_1 )                        as                                CDES_EXM_ADDR_STR_1 
		, TRIM( CDES_EXM_ADDR_STR_2 )                        as                                CDES_EXM_ADDR_STR_2 
		, TRIM( CDES_EXM_ADDR_CITY_NM )                      as                              CDES_EXM_ADDR_CITY_NM 
		, TRIM( CDES_EXM_ADDR_CNTY_NM )                      as                              CDES_EXM_ADDR_CNTY_NM 
		, TRIM( CDES_EXM_ADDR_POST_CD )                      as                              CDES_EXM_ADDR_POST_CD 
		, STT_ID                                             as                                             STT_ID 
		, TRIM( CDES_EXM_ADDR_STT_ABRV )                     as                             CDES_EXM_ADDR_STT_ABRV 
		, TRIM( CDES_EXM_ADDR_STT_NM )                       as                               CDES_EXM_ADDR_STT_NM 
		, CNTRY_ID                                           as                                           CNTRY_ID 
		, TRIM( CDES_EXM_ADDR_CNTRY_NM )                     as                             CDES_EXM_ADDR_CNTRY_NM 
		, TRIM( CDES_EXM_UN_NRML_SITU )                      as                              CDES_EXM_UN_NRML_SITU 
		, TRIM( CDES_EXM_QA )                                as                                        CDES_EXM_QA 
		, TRIM( CDES_GRTT_45_IND )                           as                                   CDES_GRTT_45_IND 
		, TRIM( CDES_TRVL_REMB_IND )                         as                                 CDES_TRVL_REMB_IND 
		, TRIM( CDES_ADDTNL_TST_IND )                        as                                CDES_ADDTNL_TST_IND 
		, TRIM( CDES_REQS_ADDTNL_TST )                       as                               CDES_REQS_ADDTNL_TST 
		, TRIM( CDES_ADNDM_REQS_IND )                        as                                CDES_ADNDM_REQS_IND 
		, TRIM( CD_ADNDM_REQS_TYP_CD )                       as                               CD_ADNDM_REQS_TYP_CD 
		, TRIM( CD_ADNDM_REQS_TYP_NM )                       as                               CD_ADNDM_REQS_TYP_NM 
		, TRIM( CDES_RSLT_SUSPD_IND )                        as                                CDES_RSLT_SUSPD_IND 
		, TRIM( CDES_ADR_NO )                                as                                        CDES_ADR_NO 
		, TRIM( CDES_ADR_TYP )                               as                                       CDES_ADR_TYP 
		, TRIM( CDES_ADR_TRT_REQSTR )                        as                                CDES_ADR_TRT_REQSTR 
		, TRIM( CDES_ADR_TRT_DSP )                           as                                   CDES_ADR_TRT_DSP 
		, AUDIT_USER_ID_CREA                                 as                                 AUDIT_USER_ID_CREA 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, AUDIT_USER_ID_UPDT                                 as                                 AUDIT_USER_ID_UPDT 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		FROM SRC_EXAM
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
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
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
		, TRIM( CLM_LOSS_DESC )                              as                                      CLM_LOSS_DESC 
		, TRIM( CLM_CLMT_JOB_TTL )                           as                                   CLM_CLMT_JOB_TTL 
        , CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND 
		FROM SRC_CLM
            )

---- RENAME LAYER ----
,

RENAME_CASES      as ( SELECT 
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
				FROM     LOGIC_CASES   ), 
RENAME_EXAM       as ( SELECT 
		  CDES_ID                                            as                                            CDES_ID
		, EXAM_CASE_DETAIL_ID                                as                                EXAM_CASE_DETAIL_ID
		, EXAM_SCHEDULE_ID                                   as                                   EXAM_SCHEDULE_ID
		, CASE_ID                                            as                                       EXAM_CASE_ID
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
		, STT_ID                                             as                                             STT_ID
		, CDES_EXM_ADDR_STT_ABRV                             as                             CDES_EXM_ADDR_STT_ABRV
		, CDES_EXM_ADDR_STT_NM                               as                               CDES_EXM_ADDR_STT_NM
		, CNTRY_ID                                           as                                           CNTRY_ID
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
		, VOID_IND                                           as                                      EXAM_VOID_IND 
				FROM     LOGIC_EXAM   ), 
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
		  CLM_NO                                             as                                         CLM_CLM_NO
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
        , CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
				FROM     LOGIC_CLM   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CASES                          as ( SELECT * FROM    RENAME_CASES 
                                            WHERE VOID_IND = 'N'  ),
FILTER_EXAM                           as ( SELECT * FROM    RENAME_EXAM 
                                            WHERE EXAM_VOID_IND = 'N'  ),
FILTER_CTH                            as ( SELECT * FROM    RENAME_CTH   ),
FILTER_CSH                            as ( SELECT * FROM    RENAME_CSH   ),
FILTER_CLM                            as ( SELECT * FROM    RENAME_CLM 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),

---- JOIN LAYER ----

CASES as ( SELECT * 
				FROM  FILTER_CASES
				INNER JOIN FILTER_EXAM ON  FILTER_CASES.CASE_ID =  FILTER_EXAM.EXAM_CASE_ID 
								LEFT JOIN FILTER_CTH ON  FILTER_CASES.CASE_CNTX_NO =  FILTER_CTH.CTH_CLM_NO AND CASE_EFF_DT BETWEEN CTH_HIST_EFF_DT AND coalesce(CTH_HIST_END_DT, '2099-12-31') 
								LEFT JOIN FILTER_CSH ON  FILTER_CASES.CASE_CNTX_NO =  FILTER_CSH.CSH_CLM_NO AND CASE_EFF_DT BETWEEN CSH_HIST_EFF_DT  AND coalesce(CSH_HIST_END_DT, '2099-12-31') 
								LEFT JOIN FILTER_CLM ON  FILTER_CASES.CASE_CNTX_NO =  FILTER_CLM.CLM_CLM_NO  )
SELECT * 
FROM CASES