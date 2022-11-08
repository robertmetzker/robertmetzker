
  create or replace  view DEV_EDW.STAGING.DSV_CLAIM  as (
    

---- SRC LAYER ----
WITH
SRC_C              as ( SELECT *     FROM     STAGING.DST_CLAIM ),
//SRC_C              as ( SELECT *     FROM     DST_CLAIM) ,

---- LOGIC LAYER ----


LOGIC_C as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, CLM_NO                                             as                                             CLM_NO 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, CLM_TYP_NM                                         as                                         CLM_TYP_NM 
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STT_TYP_NM                                     as                                     CLM_STT_TYP_NM 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_STS_TYP_NM                                     as                                     CLM_STS_TYP_NM 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, CLM_TRANS_RSN_TYP_NM                               as                               CLM_TRANS_RSN_TYP_NM 
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, OCCR_SRC_TYP_NM                                    as                                    OCCR_SRC_TYP_NM 
		, OCCR_MEDA_TYP_NM                                   as                                   OCCR_MEDA_TYP_NM 
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND 
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND 
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND 
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND 
		, COMBINED_CLAIM_IND                                 as                                 COMBINED_CLAIM_IND 
		, SB223_IND                                          as                                          SB223_IND 
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND 
		, ACCIDENT_PREMISES_TEXT                             as                             ACCIDENT_PREMISES_TEXT 
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC 
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC 
		, K_PROGRAM_START_DATE                               as                               K_PROGRAM_START_DATE 
		, K_PROGRAM_END_DATE                                 as                                 K_PROGRAM_END_DATE 
		, K_PROGRAM_REASON_CODE                              as                              K_PROGRAM_REASON_CODE 
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
		, CLM_CTRPH_INJR_IND                                 as                                 CLM_CTRPH_INJR_IND 
		, CLM_OCCR_LOC_NM                                    as                                    CLM_OCCR_LOC_NM 
		, CLM_OCCR_LOC_CITY_NM                               as                               CLM_OCCR_LOC_CITY_NM 
		, CLM_OCCR_LOC_POST_CD                               as                               CLM_OCCR_LOC_POST_CD 
		, CLM_OCCR_LOC_CNTY_NM                               as                               CLM_OCCR_LOC_CNTY_NM 
		, CLM_OCCR_LOC_STT_CD                                as                                CLM_OCCR_LOC_STT_CD 
		, CLM_OCCR_LOC_STT_NM                                as                                CLM_OCCR_LOC_STT_NM 
		, CLM_OCCR_LOC_CNTRY_NM                              as                              CLM_OCCR_LOC_CNTRY_NM 
		, CLM_OCCR_LOC_CMT                                   as                                   CLM_OCCR_LOC_CMT 
		, CLAIM_FILE_DATE                                    as                                    CLAIM_FILE_DATE 
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, CLM_FST_DCSN_DATE                                  as                                  CLM_FST_DCSN_DATE 
		, CLAIM_FILE_LAG_DAYS_COUNT                          as                          CLAIM_FILE_LAG_DAYS_COUNT 
		, ENTRY_USER_LGN_NM                                  as                                  ENTRY_USER_LGN_NM 
		, PRIMARY_ICD_CD                                     as                                     PRIMARY_ICD_CD 
		, ICD_CODE_VERSION_NUMBER                            as                            ICD_CODE_VERSION_NUMBER 
		, CLAIM_RELEASE_DATE                                 as                                 CLAIM_RELEASE_DATE 
		, CUST_NO                                            as                                            CUST_NO 
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO 
		, PRSN_BIRTH_DATE                                    as                                    PRSN_BIRTH_DATE 
		, AGE_AT_OCCURRENCE                                  as                                  AGE_AT_OCCURRENCE 
		, PLCY_NO                                            as                                            PLCY_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, NOI_CTG_TYP_NM                                     as                                     NOI_CTG_TYP_NM 
		, NOI_TYP_NM                                         as                                         NOI_TYP_NM 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE 
		, CS_CLS_CD                                          as                                          CS_CLS_CD 
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE 
		, FIRST_DETERMINATION_DATE                           as                           FIRST_DETERMINATION_DATE 
		, DETERMINATION_USER_LGN_NM                          as                          DETERMINATION_USER_LGN_NM 
		, FIRST_ASGN_EFF_DATE                                as                                FIRST_ASGN_EFF_DATE 
		, LAST_ASGN_EFF_DATE                                 as                                 LAST_ASGN_EFF_DATE 
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM 
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM 
		, CLM_CLMT_LST_WK_DATE                               as                               CLM_CLMT_LST_WK_DATE 
		, ARTW_DATE                                          as                                          ARTW_DATE 
		, ERTW_DATE                                          as                                          ERTW_DATE 
		, INTL_STLD_INDM_DATE                                as                                INTL_STLD_INDM_DATE 
		, INTL_STLD_MDCL_DATE                                as                                INTL_STLD_MDCL_DATE 
		, CLM_CLMT_NTF_DT                                    as                                    CLM_CLMT_NTF_DT 
		, CLM_EMPLR_NTF_DT                                   as                                   CLM_EMPLR_NTF_DT 
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
		, FIRST_POR_VISIT_DATE                               as                               FIRST_POR_VISIT_DATE 
		, INVOICE_DISTINCT_COUNT                             as                             INVOICE_DISTINCT_COUNT 
		, INVOICE_LINE_DISTINCT_COUNT                        as                        INVOICE_LINE_DISTINCT_COUNT 
		, MCO_NO                                             as                                             MCO_NO 
		, CLAIM_CLOSED_DATE                                  as                                  CLAIM_CLOSED_DATE 
		, TOTAL_NWWL_PAID_AMT                                as                                TOTAL_NWWL_PAID_AMT 
		, TOTAL_SCH_LOSS_PAID_AMT                            as                            TOTAL_SCH_LOSS_PAID_AMT 
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE
		, CTRPH_INJR_TYP_CD                                  as                                  CTRPH_INJR_TYP_CD
		, CTRPH_INJR_TYP_NM                                  as                                  CTRPH_INJR_TYP_NM
        , CATASTROPHIC_EFFECTIVE_DATE                        as                        CATASTROPHIC_EFFECTIVE_DATE
        , CATASTROPHIC_EXPIRATION_DATE                       as                       CATASTROPHIC_EXPIRATION_DATE
		, K_PROGRAM_ENTRY_DATE                               as                               K_PROGRAM_ENTRY_DATE 
		, AGRE_ID                                            as                                            AGRE_ID
		,PLCY_ORIG_EFF_DT                                    as                                   PLCY_ORIG_EFF_DT
		FROM SRC_C
            )

---- RENAME LAYER ----
,

RENAME_C          as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, CLM_NO                                             as                                       CLAIM_NUMBER
		, CLM_TYP_CD                                         as                  CURRENT_CORESUITE_CLAIM_TYPE_CODE
		, CLM_TYP_NM                                         as                  CURRENT_CORESUITE_CLAIM_TYPE_DESC
		, CLM_STT_TYP_CD                                     as                                   CLAIM_STATE_CODE
		, CLM_STT_TYP_NM                                     as                                   CLAIM_STATE_DESC
		, CLM_STS_TYP_CD                                     as                                  CLAIM_STATUS_CODE
		, CLM_STS_TYP_NM                                     as                                  CLAIM_STATUS_DESC
		, CLM_TRANS_RSN_TYP_CD                               as                           CLAIM_STATUS_REASON_CODE
		, CLM_TRANS_RSN_TYP_NM                               as                           CLAIM_STATUS_REASON_DESC
		, CLM_LOSS_DESC                                      as                          ACCIDENT_DESCRIPTION_TEXT
		, OCCR_SRC_TYP_NM                                    as                                 FILING_SOURCE_DESC
		, OCCR_MEDA_TYP_NM                                   as                                  FILING_MEDIA_DESC
		, FIREFIGHTER_CANCER_IND                             as                             FIREFIGHTER_CANCER_IND
		, COVID_EXPOSURE_IND                                 as                                 COVID_EXPOSURE_IND
		, COVID_EMERGENCY_WORKER_IND                         as                         COVID_EMERGENCY_WORKER_IND
		, COVID_HEALTH_CARE_WORKER_IND                       as                       COVID_HEALTH_CARE_WORKER_IND
		, COMBINED_CLAIM_IND                                 as                                       COMBINED_IND
		, SB223_IND                                          as                                          SB223_IND
		, EMPLOYER_PREMISES_IND                              as                              EMPLOYER_PREMISES_IND
		, ACCIDENT_PREMISES_TEXT                             as                             ACCIDENT_PREMISES_TEXT
		, K_PROGRAM_ENROLLMENT_DESC                          as                          K_PROGRAM_ENROLLMENT_DESC
		, K_PROGRAM_TYPE_DESC                                as                                K_PROGRAM_TYPE_DESC
		, K_PROGRAM_START_DATE                               as               EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE
		, K_PROGRAM_END_DATE                                 as              EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE
		, K_PROGRAM_REASON_CODE                              as                              K_PROGRAM_REASON_CODE
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC
		, CLM_CTRPH_INJR_IND                                 as                                   CATASTROPHIC_IND
		, CLM_OCCR_LOC_NM                                    as                             ACCIDENT_LOCATION_NAME
		, CLM_OCCR_LOC_CITY_NM                               as                             ACCIDENT_LOCATION_CITY
		, CLM_OCCR_LOC_POST_CD                               as                      ACCIDENT_LOCATION_POSTAL_CODE
		, CLM_OCCR_LOC_CNTY_NM                               as                           ACCIDENT_LOCATION_COUNTY
		, CLM_OCCR_LOC_STT_CD                                as                       ACCIDENT_LOCATION_STATE_CODE
		, CLM_OCCR_LOC_STT_NM                                as                            ACCIDENT_LOCATION_STATE
		, CLM_OCCR_LOC_CNTRY_NM                              as                          ACCIDENT_LOCATION_COUNTRY
		, CLM_OCCR_LOC_CMT                                   as                             ACCIDENT_LOCATION_TEXT
		, CLAIM_FILE_DATE                                    as                                    CLAIM_FILE_DATE
		, CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, AUDIT_USER_CREA_DTM                                as                            CLM_AUDIT_USER_CREA_DTM
		, CLM_FST_DCSN_DATE                                  as                            CLM_FIRST_DECISION_DATE
		, CLAIM_FILE_LAG_DAYS_COUNT                          as                          CLAIM_FILE_LAG_DAYS_COUNT
		, ENTRY_USER_LGN_NM                                  as                              CLM_ENTRY_USER_LGN_NM
		, PRIMARY_ICD_CD                                     as                                   PRIMARY_ICD_CODE
		, ICD_CODE_VERSION_NUMBER                            as                            ICD_CODE_VERSION_NUMBER
		, CLAIM_RELEASE_DATE                                 as                                 CLAIM_RELEASE_DATE
		, CUST_NO                                            as                                 IW_CUSTOMER_NUMBER
		, EMP_CUST_NO                                        as                                EMP_CUSTOMER_NUMBER
		, PRSN_BIRTH_DATE                                    as                                      IW_BIRTH_DATE
		, AGE_AT_OCCURRENCE                                  as                               IW_AGE_AT_OCCURRENCE
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, BUSN_SEQ_NO                                        as                                BUSINESS_SEQ_NUMBER
		, NOI_CTG_TYP_NM                                     as                          NATURE_OF_INJURY_CATEGORY
		, NOI_TYP_NM                                         as                              NATURE_OF_INJURY_TYPE
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, CLAIM_INITIAL_FILE_DATE                            as                            CLAIM_INITIAL_FILE_DATE
		, CS_CLS_CD                                          as                                  MANUAL_CLASS_CODE
		, DRVD_MANUAL_CLASS_SUFFIX_CODE                      as                      DRVD_MANUAL_CLASS_SUFFIX_CODE
		, FIRST_DETERMINATION_DATE                           as                           FIRST_DETERMINATION_DATE
		, DETERMINATION_USER_LGN_NM                          as                          DETERMINATION_USER_LGN_NM
		, FIRST_ASGN_EFF_DATE                                as                              FIRST_ASSIGNMENT_DATE
		, LAST_ASGN_EFF_DATE                                 as                               LAST_ASSIGNMENT_DATE
		, ORG_UNT_NM                                         as                                         ORG_UNT_NM
		, ORG_UNT_ABRV_NM                                    as                                    ORG_UNT_ABRV_NM
		, CLM_CLMT_LST_WK_DATE                               as                              LAST_DAY_AT_WORK_DATE
		, ARTW_DATE                                          as                         ACTUAL_RETURN_TO_WORK_DATE
		, ERTW_DATE                                          as                      ESTIMATED_RETURN_TO_WORK_DATE
		, INTL_STLD_INDM_DATE                                as                                INTL_STLD_INDM_DATE
		, INTL_STLD_MDCL_DATE                                as                                INTL_STLD_MDCL_DATE
		, CLM_CLMT_NTF_DT                                    as                                    CLM_CLMT_NTF_DT
		, CLM_EMPLR_NTF_DT                                   as                                   CLM_EMPLR_NTF_DT 
		, CHNG_OVR_IND                                       as                            CLAIM_TYPE_CHNG_OVR_IND
		, CLM_CLMT_JOB_TTL                                   as                                       IW_JOB_TITLE
		, FIRST_POR_VISIT_DATE                               as                               FIRST_POR_VISIT_DATE
		, INVOICE_DISTINCT_COUNT                             as                             INVOICE_DISTINCT_COUNT
		, INVOICE_LINE_DISTINCT_COUNT                        as                        INVOICE_LINE_DISTINCT_COUNT
		, MCO_NO                                             as                                             MCO_NO
		, CLAIM_CLOSED_DATE                                  as                                  CLAIM_CLOSED_DATE
		, TOTAL_NWWL_PAID_AMT                                as                                TOTAL_NWWL_PAID_AMT
		, TOTAL_SCH_LOSS_PAID_AMT                            as                            TOTAL_SCH_LOSS_PAID_AMT
		, INDUSTRY_GROUP_CODE                                as                                INDUSTRY_GROUP_CODE
		, CTRPH_INJR_TYP_CD                                  as                      CATASTROPHIC_INJURY_TYPE_CODE
		, CTRPH_INJR_TYP_NM                                  as                      CATASTROPHIC_INJURY_TYPE_DESC
        , CATASTROPHIC_EFFECTIVE_DATE                        as                        CATASTROPHIC_EFFECTIVE_DATE
        , CATASTROPHIC_EXPIRATION_DATE                       as                       CATASTROPHIC_EXPIRATION_DATE
		, K_PROGRAM_ENTRY_DATE                               as         EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE
		, AGRE_ID                                            as                                            AGRE_ID
		,PLCY_ORIG_EFF_DT                                    as                     POLICY_ORIGINAL_EFFECTIVE_DATE
				FROM     LOGIC_C   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_C                              as ( SELECT * FROM    RENAME_C   ),

---- JOIN LAYER ----

 JOIN_C           as  ( SELECT * 
				FROM  FILTER_C )
 SELECT * FROM  JOIN_C
  );
