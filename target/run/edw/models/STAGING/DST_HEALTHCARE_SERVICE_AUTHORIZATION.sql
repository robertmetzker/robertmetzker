

      create or replace  table DEV_EDW.STAGING.DST_HEALTHCARE_SERVICE_AUTHORIZATION  as
      (---- SRC LAYER ----
 WITH
SRC_CS as ( SELECT *     from     STAGING.STG_CASE_SERVICE ),
SRC_CSS as ( SELECT *     from     STAGING.STG_CASES ),
SRC_CTH as ( SELECT *     from     STAGING.DST_CLAIM_TYPE_HISTORY ),
SRC_CSH as ( SELECT *     from     STAGING.DST_CLAIM_STATUS_HISTORY ),
SRC_CLM as ( SELECT *     from     STAGING.DST_CLAIM ),
SRC_CASE_HIST      as ( SELECT *     from     STAGING.STG_CASE_STATUS_HISTORY ),
//SRC_CS as ( SELECT *     from     STG_CASE_SERVICE) ,
//SRC_CSS as ( SELECT *     from     STG_CASES) ,
//SRC_CTH as ( SELECT *     from     DST_CLAIM_TYPE_HISTORY) ,
//SRC_CSH as ( SELECT *     from     DST_CLAIM_STATUS_HISTORY) ,
//SRC_CLM as ( SELECT *     from     DST_CLAIM) ,
//SRC_CASE_HIST      as ( SELECT *     from     STG_CASE_STATUS_HISTORY) ,

---- LOGIC LAYER ----


LOGIC_CS as ( SELECT 
		  CASE_SERV_ID                                       as                                       CASE_SERV_ID 
		, CASE_ID                                            as                                            CASE_ID 
		, TRIM( CASE_SERV_AUTH_NO )                          as                                  CASE_SERV_AUTH_NO 
		, CASE_SERV_AUTH_DT                                  as                                  CASE_SERV_AUTH_DT 
		, CASE_SERV_APRV_FR_DT                               as                               CASE_SERV_APRV_FR_DT 
		, CASE_SERV_APRV_TO_DT                               as                               CASE_SERV_APRV_TO_DT 
		, TRIM( CASE_SERV_TYP_CD )                           as                                   CASE_SERV_TYP_CD 
		, TRIM( CASE_SERV_STS_TYP_CD )                       as                               CASE_SERV_STS_TYP_CD 
		, TRIM( CSD_CD_FR )                                  as                                          CSD_CD_FR 
		, TRIM( CSD_CD_TO )                                  as                                          CSD_CD_TO 
		, TRIM(VOID_IND)                                     as                                           VOID_IND
		from SRC_CS
            ),

LOGIC_CSS as ( SELECT 
		  TRIM( CASE_NO )                                    as                                            CASE_NO 
		, CASE_ID                                            as                                            CASE_ID 
		, TRIM( CASE_CNTX_NO )                               as                                       CASE_CNTX_NO 
		, TRIM( CASE_TYP_CD )                                as                                        CASE_TYP_CD 
		, TRIM( CASE_CTG_TYP_CD )                            as                                    CASE_CTG_TYP_CD 
		, TRIM( APP_CNTX_TYP_CD )                            as                                    APP_CNTX_TYP_CD 
		, TRIM( CASE_PRTY_TYP_CD )                           as                                   CASE_PRTY_TYP_CD 
		, TRIM( CASE_RSOL_TYP_CD )                           as                                   CASE_RSOL_TYP_CD 
		, CASE_INT_DT                                        as                                        CASE_INT_DT 
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT 
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT 
		, CASE_COMP_DT                                       as                                       CASE_COMP_DT 
		from SRC_CSS
            ),

LOGIC_CTH as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, TRIM( CLM_TYP_NM )                                 as                                         CLM_TYP_NM 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		, TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, TRIM( CHNG_OVR_IND )                               as                                       CHNG_OVR_IND 
		from SRC_CTH
            ),

LOGIC_CSH as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, TRIM( CLM_STS_TYP_CD )                             as                                     CLM_STS_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		from SRC_CSH
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
		, TRIM( OCCR_PRMS_TYP_NM )                           as                                   OCCR_PRMS_TYP_NM 
		, TRIM( CLM_OCCR_LOC_CNTRY_NM )                      as                              CLM_OCCR_LOC_CNTRY_NM 
		, TRIM( CLM_OCCR_LOC_STT_CD )                        as                                CLM_OCCR_LOC_STT_CD 
		, TRIM( CLM_OCCR_LOC_STT_NM )                        as                                CLM_OCCR_LOC_STT_NM 
		, TRIM( CLM_OCCR_LOC_CNTY_NM )                       as                               CLM_OCCR_LOC_CNTY_NM 
		, TRIM( CLM_OCCR_LOC_CITY_NM )                       as                               CLM_OCCR_LOC_CITY_NM 
		, TRIM( CLM_OCCR_LOC_POST_CD )                       as                               CLM_OCCR_LOC_POST_CD 
		, TRIM( CLM_OCCR_LOC_NM )                            as                                    CLM_OCCR_LOC_NM 
		, TRIM( CLM_OCCR_LOC_STR_1 )                         as                                 CLM_OCCR_LOC_STR_1 
		, TRIM( CLM_OCCR_LOC_STR_2 )                         as                                 CLM_OCCR_LOC_STR_2 
		, TRIM( CLM_OCCR_LOC_CMT )                           as                                   CLM_OCCR_LOC_CMT 
		FROM SRC_CLM
            ),

LOGIC_CASE_HIST as ( SELECT 
		  CASE_ID                                            as                                            CASE_ID 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		, TRIM( CASE_STT_TYP_CD )                            as                                    CASE_STT_TYP_CD 
		, CASE_STT_STS_STT_EFF_DT                            as                            CASE_STT_STS_STT_EFF_DT 
		, TRIM( CASE_STS_TYP_CD )                            as                                    CASE_STS_TYP_CD 
		, CASE_STT_STS_STS_EFF_DT                            as                            CASE_STT_STS_STS_EFF_DT 
		, TRIM( CASE_STS_RSN_TYP_CD )                        as                                CASE_STS_RSN_TYP_CD 
		, CASE_STT_STS_STS_RSN_EFF_DT                        as                        CASE_STT_STS_STS_RSN_EFF_DT 
		, HIST_EFF_DTM                                       as                                       HIST_EFF_DTM 
		, HIST_END_DTM                                       as                                       HIST_END_DTM 
		FROM SRC_CASE_HIST
            )

---- RENAME LAYER ----
,

RENAME_CS as ( SELECT 
		  CASE_SERV_ID                                       as                                       CASE_SERV_ID
		, CASE_ID                                            as                                            CASE_ID
		, CASE_SERV_AUTH_NO                                  as                                  CASE_SERV_AUTH_NO
		, CASE_SERV_AUTH_DT                                  as                                  CASE_SERV_AUTH_DT
		, CASE_SERV_APRV_FR_DT                               as                               CASE_SERV_APRV_FR_DT
		, CASE_SERV_APRV_TO_DT                               as                               CASE_SERV_APRV_TO_DT
		, CASE_SERV_TYP_CD                                   as                                   CASE_SERV_TYP_CD
		, CASE_SERV_STS_TYP_CD                               as                               CASE_SERV_STS_TYP_CD
		, CSD_CD_FR                                          as                                          CSD_CD_FR
		, CSD_CD_TO                                          as                                          CSD_CD_TO
        , VOID_IND                                           as                                           VOID_IND		 
				FROM     LOGIC_CS   ), 

RENAME_CSS as ( SELECT
          CASE_NO                                            as                                            CASE_NO
		, CASE_ID                                            as                                        CSS_CASE_ID
        , CASE_CNTX_NO                                       as                                       CASE_CNTX_NO
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD 
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD 
		, CASE_INT_DT                                        as                                        CASE_INT_DT
		, CASE_EFF_DT                                        as                                        CASE_EFF_DT
		, CASE_DUE_DT                                        as                                        CASE_DUE_DT
		, CASE_COMP_DT                                       as                                       CASE_COMP_DT 
				FROM     LOGIC_CSS   ), 
RENAME_CTH as ( SELECT 
		  CLM_NO                                             as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM
		, HIST_EFF_DT                                        as                                    CTH_HIST_EFF_DT
		, HIST_END_DT                                        as                                    CTH_HIST_END_DT
		, CLM_REL_SNPSHT_IND                                 as                             CTH_CLM_REL_SNPSHT_IND
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
				FROM     LOGIC_CTH   ), 
RENAME_CSH as ( SELECT 
		  CLM_NO                                             as                                         CSH_CLM_NO
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, HIST_EFF_DT                                        as                                    CSH_HIST_EFF_DT
		, HIST_END_DT                                        as                                    CSH_HIST_END_DT 
				FROM     LOGIC_CSH   ), 
RENAME_CLM as ( SELECT 
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
		, OCCR_PRMS_TYP_NM                                   as                                   OCCR_PRMS_TYP_NM
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM
		, CLM_OCCR_LOC_STR_1                                 as                                 CLM_OCCR_LOC_STR_1
		, CLM_OCCR_LOC_STR_2                                 as                                 CLM_OCCR_LOC_STR_2
		, CLM_OCCR_LOC_CMT                                   as                                  CLM_OCCR_LOC_COMT 
				FROM     LOGIC_CLM   ), 
RENAME_CASE_HIST  as ( SELECT 
		  CASE_ID                                            as                                  CASE_HIST_CASE_ID
		, VOID_IND                                           as                                 CASE_HIST_VOID_IND
		, CASE_STT_TYP_CD                                    as                                    CASE_STT_TYP_CD
		, CASE_STT_STS_STT_EFF_DT                            as                            CASE_STT_STS_STT_EFF_DT
		, CASE_STS_TYP_CD                                    as                                    CASE_STS_TYP_CD
		, CASE_STT_STS_STS_EFF_DT                            as                            CASE_STT_STS_STS_EFF_DT
		, CASE_STS_RSN_TYP_CD                                as                                CASE_STS_RSN_TYP_CD
		, CASE_STT_STS_STS_RSN_EFF_DT                        as                        CASE_STT_STS_STS_RSN_EFF_DT
		, HIST_EFF_DTM                                       as                             CASE_HIST_HIST_EFF_DTM
		, HIST_END_DTM                                       as                             CASE_HIST_HIST_END_DTM 
				FROM     LOGIC_CASE_HIST   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CS                             as ( SELECT * from    RENAME_CS   ),
FILTER_CSS                            as ( SELECT * from    RENAME_CSS   ),
FILTER_CTH                            as ( SELECT * from    RENAME_CTH   ),
FILTER_CSH                            as ( SELECT * from    RENAME_CSH   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_CASE_HIST                      as ( SELECT * from   RENAME_CASE_HIST 
                                            WHERE CASE_HIST_HIST_END_DTM IS NULL AND CASE_HIST_VOID_IND = 'N'  ),


---- JOIN LAYER ----

CS as ( SELECT * 
				FROM  FILTER_CS
				LEFT JOIN FILTER_CSS ON  FILTER_CS.CASE_ID = FILTER_CSS.CSS_CASE_ID  ),

CSS as ( SELECT * 
				FROM  CS
				LEFT JOIN FILTER_CASE_HIST ON  CS.CASE_ID =  FILTER_CASE_HIST.CASE_HIST_CASE_ID
				LEFT JOIN FILTER_CTH ON  CS.CASE_CNTX_NO =  FILTER_CTH.CTH_CLM_NO AND CS.CASE_SERV_AUTH_DT BETWEEN FILTER_CTH.CTH_HIST_EFF_DT AND coalesce(FILTER_CTH.CTH_HIST_END_DT, '2099-12-31') 
								LEFT JOIN FILTER_CSH ON  CS.CASE_CNTX_NO =  FILTER_CSH.CSH_CLM_NO AND CS.CASE_SERV_AUTH_DT BETWEEN FILTER_CSH.CSH_HIST_EFF_DT  AND coalesce(FILTER_CSH.CSH_HIST_END_DT, '2099-12-31') 
								LEFT JOIN FILTER_CLM ON  CS.CASE_CNTX_NO =  FILTER_CLM.CLM_CLM_NO  )

SELECT 
  CASE_SERV_ID
, CASE_ID
, CASE_NO
, CASE_SERV_AUTH_NO
, CASE_SERV_AUTH_DT
, CASE_SERV_APRV_FR_DT
, CASE_SERV_APRV_TO_DT
, CASE_SERV_TYP_CD
, CASE_SERV_STS_TYP_CD
, CSD_CD_FR
, CSD_CD_TO
, VOID_IND
, CSS_CASE_ID
, CASE_CNTX_NO
, CASE_TYP_CD
, CASE_CTG_TYP_CD
, APP_CNTX_TYP_CD
, CASE_PRTY_TYP_CD
, CASE_RSOL_TYP_CD
, CTH_CLM_NO
, CLM_TYP_CD
, CLM_TYP_NM
, CTH_HIST_EFF_DT
, CTH_HIST_END_DT
, CHNG_OVR_IND
, CSH_CLM_NO
, CLM_STT_TYP_CD
, CLM_STS_TYP_CD
, CLM_TRANS_RSN_TYP_CD
, CSH_HIST_EFF_DT
, CSH_HIST_END_DT
, CLM_CLM_NO
, OCCR_SRC_TYP_NM
, OCCR_MEDA_TYP_NM
, NOI_CTG_TYP_NM
, NOI_TYP_NM
, FIREFIGHTER_CANCER_IND
, COVID_EXPOSURE_IND
, COVID_EMERGENCY_WORKER_IND
, COVID_HEALTH_CARE_WORKER_IND
, COMBINED_CLAIM_IND
, SB223_IND
, EMPLOYER_PREMISES_IND
, CLM_CTRPH_INJR_IND
, K_PROGRAM_ENROLLMENT_DESC
, K_PROGRAM_TYPE_DESC
, K_PROGRAM_START_DATE
, K_PROGRAM_END_DATE
, K_PROGRAM_REASON_CODE
, K_PROGRAM_REASON_DESC
, CUST_NO
, EMP_CUST_NO
, PLCY_NO
, BUSN_SEQ_NO
, POLICY_TYPE_CODE
, PLCY_STS_TYP_CD
, PLCY_STS_RSN_TYP_CD
, POLICY_ACTIVE_IND
, CLM_LOSS_DESC
, CLM_CLMT_JOB_TTL
, OCCR_PRMS_TYP_NM
, CLM_OCCR_LOC_CNTRY_NM
, CLM_OCCR_LOC_STT_CD
, CLM_OCCR_LOC_STT_NM
, CLM_OCCR_LOC_CNTY_NM
, CLM_OCCR_LOC_CITY_NM
, CLM_OCCR_LOC_POST_CD
, CLM_OCCR_LOC_NM
, CLM_OCCR_LOC_STR_1
, CLM_OCCR_LOC_STR_2
, CLM_OCCR_LOC_COMT
, CASE_HIST_CASE_ID
, CASE_HIST_VOID_IND
, CASE_STT_TYP_CD
, CASE_STT_STS_STT_EFF_DT
, CASE_STS_TYP_CD
, CASE_STT_STS_STS_EFF_DT
, CASE_STS_RSN_TYP_CD
, CASE_STT_STS_STS_RSN_EFF_DT
, CASE_HIST_HIST_EFF_DTM
, CASE_HIST_HIST_END_DTM
, CASE_INT_DT
, CASE_EFF_DT
, CASE_DUE_DT
, CASE_COMP_DT
from CSS
      );
    