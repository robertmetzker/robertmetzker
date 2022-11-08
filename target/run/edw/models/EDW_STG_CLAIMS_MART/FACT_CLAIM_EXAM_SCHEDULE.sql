

      create or replace  table DEV_EDW.EDW_STG_CLAIMS_MART.FACT_CLAIM_EXAM_SCHEDULE  as
      (

---- SRC LAYER ----
WITH
SRC_CES            as ( SELECT *     FROM     STAGING.DSV_CASE_EXAM_SCHEDULE ),
SRC_EMP            as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_EMPLOYER ),
SRC_IW             as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
//SRC_CES            as ( SELECT *     FROM     DSV_CASE_EXAM_SCHEDULE) ,
//SRC_EMP            as ( SELECT *     FROM     DIM_EMPLOYER) ,
//SRC_IW             as ( SELECT *     FROM     DIM_INJURED_WORKER) ,

---- LOGIC LAYER ----


LOGIC_CES as ( SELECT 
		  CASE_NUMBER                                        as                                        CASE_NUMBER 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( APP_CNTX_TYP_CD, CASE_CTG_TYP_CD, CASE_TYP_CD, CASE_PRTY_TYP_CD, CASE_RSOL_TYP_CD ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(APP_CNTX_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_CTG_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_PRTY_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_RSOL_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                                     CASE_TYPE_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CD_EXM_REQS_TYP_CD, CPS_TYP_CD, CPS_TYP_CD_SCND, CD_ADNDM_REQS_TYP_CD, LANG_TYP_CD ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CD_EXM_REQS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CPS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CPS_TYP_CD_SCND as 
    varchar
), '') || '-' || coalesce(cast(CD_ADNDM_REQS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(LANG_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                              EXAM_CASE_DETAIL_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CDES_CLMT_AVL_MON_IND, CDES_CLMT_AVL_TUE_IND, CDES_CLMT_AVL_WED_IND, CDES_CLMT_AVL_THU_IND, CDES_CLMT_AVL_FRI_IND, CDES_CLMT_AVL_SAT_IND, CDES_CLMT_AVL_SUN_IND, CDES_ITPRT_NEED_IND, CDES_GRTT_45_IND, CDES_TRVL_REMB_IND, CDES_ADDTNL_TST_IND, CDES_ADNDM_REQS_IND, CDES_RSLT_SUSPD_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
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
				END                                         
                                                             as                                 EXAM_SCHEDULE_HKEY 
		, CASE WHEN CASE_EFF_DT is null then '-1' 
			   WHEN CASE_EFF_DT < '1901-01-01' then '-2' 
			   WHEN CASE_EFF_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_EFF_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                            CASE_EFFECTIVE_DATE_KEY 
		, CASE WHEN CASE_DUE_DT is null then '-1' 
			   WHEN CASE_DUE_DT < '1901-01-01' then '-2' 
			   WHEN CASE_DUE_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_DUE_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                  CASE_DUE_DATE_KEY 
		, CASE WHEN CASE_COMPLETE_DATE is null then '-1' 
			   WHEN CASE_COMPLETE_DATE < '1901-01-01' then '-2' 
			   WHEN CASE_COMPLETE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CASE_COMPLETE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                             CASE_COMPLETE_DATE_KEY 
		, CASE WHEN CDES_EXM_DT is null then '-1' 
			   WHEN CDES_EXM_DT < '1901-01-01' then '-2' 
			   WHEN CDES_EXM_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( CDES_EXM_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                      EXAM_DATE_KEY 
		, CASE WHEN CDES_EXM_RPT_RECV_DT is null then '-1' 
			   WHEN CDES_EXM_RPT_RECV_DT < '1901-01-01' then '-2' 
			   WHEN CDES_EXM_RPT_RECV_DT > '2099-12-31' then '-3' 
			ELSE regexp_replace( CDES_EXM_RPT_RECV_DT, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                       EXAM_REPORT_RECEIVED_DATE_KEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(POLICY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PLCY_STS_RSN_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(POLICY_ACTIVE_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                               POLICY_STANDING_HKEY  
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CLM_LOSS_DESC, CLM_CLMT_JOB_TTL ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CLM_LOSS_DESC as 
    varchar
), '') || '-' || coalesce(cast(CLM_CLMT_JOB_TTL as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                           CLAIM_ACCIDENT_DESC_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CLM_TYP_CD, CHNG_OVR_IND, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
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
				END                                         
                                                             as                             CLAIM_TYPE_STATUS_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( OCCR_SRC_TYP_NM, OCCR_MEDA_TYP_NM, NOI_CTG_TYP_NM, NOI_TYP_NM, FIREFIGHTER_CANCER_IND, COVID_EXPOSURE_IND, COVID_EMERGENCY_WORKER_IND, COVID_HEALTH_CARE_WORKER_IND, COMBINED_CLAIM_IND, SB223_IND, EMPLOYER_PREMISES_IND, CLM_CTRPH_INJR_IND, K_PROGRAM_ENROLLMENT_DESC, K_PROGRAM_TYPE_DESC, K_PROGRAM_REASON_DESC ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
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
				END                                         
                                                             as                                  CLAIM_DETAIL_HKEY 
		, CASE_CLAIM_NUMBER                                  as                                  CASE_CLAIM_NUMBER 
		, PLCY_NO                                            as                                            PLCY_NO 
		, CDES_EXM_PHYS_IMPR_RT                              as                              CDES_EXM_PHYS_IMPR_RT 
		, CDES_EXM_FNL_IMPR_RT                               as                               CDES_EXM_FNL_IMPR_RT 
		, CDES_ADR_NO                                        as                                        CDES_ADR_NO 
	/*	, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
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
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC */
		, CUST_NO                                            as                                            CUST_NO 
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO 
	/*	, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
		, CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND 
		, CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND 
		, CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND 
		, CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND 
		, CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND 
		, CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND 
		, CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND 
		, CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND 
		, CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND 
		, CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND 
		, CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND 
		, CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND 
		, CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND 
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD 
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD 
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD 
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD 
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD 
		, CASE_SRC_TYP_CD                                    as                                    CASE_SRC_TYP_CD 
		, CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD 
		, CPS_TYP_CD                                         as                                         CPS_TYP_CD 
		, CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND 
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD 
		, LANG_TYP_CD                                        as                                        LANG_TYP_CD  */
	    , CASE_EFF_DT                                        as                                        CASE_EFF_DT 
		FROM SRC_CES
            ),

LOGIC_EMP as ( SELECT 
		  COALESCE( EMPLOYER_HKEY, MD5( '99999' ) )          as                                      EMPLOYER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                                    as                                    RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
 		FROM SRC_EMP
            ),

LOGIC_IW as ( SELECT 
		  COALESCE( INJURED_WORKER_HKEY, MD5( '99999' ) )    as                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		FROM SRC_IW
            )

---- RENAME LAYER ----
,

RENAME_CES        as ( SELECT 
		  CASE_NUMBER                                        as                                        CASE_NUMBER
		, CASE_TYPE_HKEY                                     as                                     CASE_TYPE_HKEY
		, EXAM_CASE_DETAIL_HKEY                              as                              EXAM_CASE_DETAIL_HKEY
		, EXAM_SCHEDULE_HKEY                                 as                                 EXAM_SCHEDULE_HKEY
		, CASE_EFFECTIVE_DATE_KEY                            as                            CASE_EFFECTIVE_DATE_KEY
		, CASE_DUE_DATE_KEY                                  as                                  CASE_DUE_DATE_KEY
		, CASE_COMPLETE_DATE_KEY                             as                             CASE_COMPLETE_DATE_KEY
		, EXAM_DATE_KEY                                      as                                      EXAM_DATE_KEY
		, EXAM_REPORT_RECEIVED_DATE_KEY                      as                      EXAM_REPORT_RECEIVED_DATE_KEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, CLAIM_ACCIDENT_DESC_HKEY                           as                           CLAIM_ACCIDENT_DESC_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, CASE_CLAIM_NUMBER                                  as                                       CLAIM_NUMBER
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, CDES_EXM_PHYS_IMPR_RT                              as                        PHYSICAN_IMPAIREMENT_RATING
		, CDES_EXM_FNL_IMPR_RT                               as                           FINAL_IMPAIREMENT_RATING
		, CDES_ADR_NO                                        as                                         ADR_NUMBER
	/*	, CLM_TYP_CD                                         as                                         CLM_TYP_CD
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
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC */
		, CUST_NO                                            as                                            CUST_NO
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO
	/*	, PLCY_NO                                            as                                            PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL
		, CDES_CLMT_AVL_MON_IND                              as                              CDES_CLMT_AVL_MON_IND
		, CDES_CLMT_AVL_TUE_IND                              as                              CDES_CLMT_AVL_TUE_IND
		, CDES_CLMT_AVL_WED_IND                              as                              CDES_CLMT_AVL_WED_IND
		, CDES_CLMT_AVL_THU_IND                              as                              CDES_CLMT_AVL_THU_IND
		, CDES_CLMT_AVL_FRI_IND                              as                              CDES_CLMT_AVL_FRI_IND
		, CDES_CLMT_AVL_SAT_IND                              as                              CDES_CLMT_AVL_SAT_IND
		, CDES_CLMT_AVL_SUN_IND                              as                              CDES_CLMT_AVL_SUN_IND
		, CDES_ITPRT_NEED_IND                                as                                CDES_ITPRT_NEED_IND
		, CDES_GRTT_45_IND                                   as                                   CDES_GRTT_45_IND
		, CDES_TRVL_REMB_IND                                 as                                 CDES_TRVL_REMB_IND
		, CDES_ADDTNL_TST_IND                                as                                CDES_ADDTNL_TST_IND
		, CDES_ADNDM_REQS_IND                                as                                CDES_ADNDM_REQS_IND
		, CDES_RSLT_SUSPD_IND                                as                                CDES_RSLT_SUSPD_IND
		, APP_CNTX_TYP_CD                                    as                                    APP_CNTX_TYP_CD
		, CASE_CTG_TYP_CD                                    as                                    CASE_CTG_TYP_CD
		, CASE_TYP_CD                                        as                                        CASE_TYP_CD
		, CASE_PRTY_TYP_CD                                   as                                   CASE_PRTY_TYP_CD
		, CASE_RSOL_TYP_CD                                   as                                   CASE_RSOL_TYP_CD
		, CASE_SRC_TYP_CD                                    as                                    CASE_SRC_TYP_CD
		, CD_EXM_REQS_TYP_CD                                 as                                 CD_EXM_REQS_TYP_CD
		, CPS_TYP_CD                                         as                                         CPS_TYP_CD
		, CPS_TYP_CD_SCND                                    as                                    CPS_TYP_CD_SCND
		, CD_ADNDM_REQS_TYP_CD                               as                               CD_ADNDM_REQS_TYP_CD
		, LANG_TYP_CD                                        as                                        LANG_TYP_CD */
	    , CASE_EFF_DT                                        as                                        CASE_EFF_DT
				FROM     LOGIC_CES   ), 
RENAME_EMP        as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                          EMP_RECORD_EFFECTIVE_DATE
        , RECORD_END_DATE                                    as                                EMP_RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                             EMP_CURRENT_RECORD_IND
				FROM     LOGIC_EMP   ), 
RENAME_IW         as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    as                       IW_CORESUITE_CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                           IW_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                 IW_RECORD_END_DATE
		, CURRENT_RECORD_IND                                 as                              IW_CURRENT_RECORD_IND
				FROM     LOGIC_IW   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CES                            as ( SELECT * FROM    RENAME_CES   ),
FILTER_IW                             as ( SELECT * FROM    RENAME_IW   ),
FILTER_EMP                            as ( SELECT * FROM    RENAME_EMP   ),

---- JOIN LAYER ----

CES as ( SELECT * 
				FROM  FILTER_CES
				LEFT JOIN FILTER_IW ON  coalesce( FILTER_CES.CUST_NO, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND CASE_EFF_DT BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_EMP ON  coalesce( FILTER_CES.EMP_CUST_NO, '99999') =  FILTER_EMP.CUSTOMER_NUMBER AND CASE_EFF_DT  BETWEEN EMP_RECORD_EFFECTIVE_DATE AND coalesce( EMP_RECORD_END_DATE, '2099-12-31')  )

-- ETL join layer to handle that are outside of date range MD5('-2222')    
,
ETL_CES as ( SELECT FIL_CES.* 
					, FILTER_IW_CES.IW_CORESUITE_CUSTOMER_NUMBER AS FILTER_IW_CES_CORESUITE_CUSTOMER_NUMBER
					, FILTER_EMP_CES.CUSTOMER_NUMBER AS FILTER_EMP_CES_CUSTOMER_NUMBER
				FROM  CES FIL_CES
				LEFT JOIN FILTER_IW FILTER_IW_CES ON  coalesce( FIL_CES.CUST_NO, '7777') =  FILTER_IW_CES.IW_CORESUITE_CUSTOMER_NUMBER AND FILTER_IW_CES.IW_CURRENT_RECORD_IND = 'Y'
				LEFT JOIN FILTER_EMP FILTER_EMP_CES ON  coalesce( FIL_CES.EMP_CUST_NO, '7777') =  FILTER_EMP_CES.CUSTOMER_NUMBER AND  FILTER_EMP_CES.EMP_CURRENT_RECORD_IND = 'Y'  ),

---- ETL LAYER ----
ETL AS ( SELECT 
         CASE_NUMBER
       , CASE_TYPE_HKEY
       , EXAM_CASE_DETAIL_HKEY
       , EXAM_SCHEDULE_HKEY
       , CASE_EFFECTIVE_DATE_KEY
       , CASE_DUE_DATE_KEY
       , CASE_COMPLETE_DATE_KEY
       , EXAM_DATE_KEY
       , EXAM_REPORT_RECEIVED_DATE_KEY
       , CASE WHEN CUSTOMER_NUMBER IS NOT NULL THEN EMPLOYER_HKEY
              WHEN FILTER_EMP_CES_CUSTOMER_NUMBER IS NOT NULL THEN MD5('-2222')
        	  ELSE MD5('-1111') END AS EMPLOYER_HKEY 
       , POLICY_STANDING_HKEY
	   , CASE WHEN IW_CORESUITE_CUSTOMER_NUMBER IS NOT NULL THEN INJURED_WORKER_HKEY
                WHEN FILTER_IW_CES_CORESUITE_CUSTOMER_NUMBER IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS INJURED_WORKER_HKEY 
       , CLAIM_ACCIDENT_DESC_HKEY
       , CLAIM_TYPE_STATUS_HKEY
       , CLAIM_DETAIL_HKEY
       , CLAIM_NUMBER
       , POLICY_NUMBER
       , PHYSICAN_IMPAIREMENT_RATING
       , FINAL_IMPAIREMENT_RATING
       , ADR_NUMBER 
	   , CURRENT_TIMESTAMP AS LOAD_DATETIME
	   , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
	   , 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM
    from ETL_CES
)

SELECT * FROM ETL
      );
    