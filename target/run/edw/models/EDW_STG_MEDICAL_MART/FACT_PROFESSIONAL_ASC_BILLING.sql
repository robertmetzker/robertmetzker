

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FACT_PROFESSIONAL_ASC_BILLING  as
      (

---- SRC LAYER ----
WITH
SRC_INVOICE as ( SELECT *     from     STAGING.DSV_INVOICE ),
SRC_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_S_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_P_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_CPT as ( SELECT *     from     EDW_STAGING_DIM.DIM_CPT ),
SRC_PL_SVC as ( SELECT *     from     EDW_STAGING_DIM.DIM_PLACE_OF_SERVICE ),
SRC_R_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_P_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD1 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD2 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD3 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD4 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_IW as ( SELECT *     from     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
--SRC_CLM as ( SELECT *     from     EDW_STAGING_DIM.DIM_CLAIM ),
//SRC_INVOICE as ( SELECT *     from     DSV_INVOICE) ,
//SRC_NTWK as ( SELECT *     from     DIM_NETWORK) ,
//SRC_S_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_P_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_CPT as ( SELECT *     from     DIM_CPT) ,
//SRC_PL_SVC as ( SELECT *     from     DIM_PLACE_OF_SERVICE) ,
//SRC_R_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_P_ICD as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD1 as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD2 as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD3 as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD4 as ( SELECT *     from     DIM_ICD) ,
//SRC_P_NTWK as ( SELECT *     from     DIM_NETWORK) ,
//SRC_IW as ( SELECT *     from     DIM_INJURED_WORKER) ,
--//SRC_CLM as ( SELECT *     from     DIM_CLAIM) ,

---- LOGIC LAYER ----

LOGIC_INVOICE as ( SELECT 
		  INVOICE_NUMBER                                     as                             MEDICAL_INVOICE_NUMBER 
		, LINE_SEQUENCE                                      as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, SEQUENCE_EXTENSION                                 as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER 
		, LINE_VERSION_NUMBER                                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER 
		, CASE WHEN LINE_DLM is null then '-1' 
			WHEN LINE_DLM < '1901-01-01' then '-2' 
			WHEN LINE_DLM > '2099-12-31' then '-3' 
			ELSE regexp_replace( LINE_DLM, '[^0-9]+', '') 
				END :: INTEGER
		                                                     as                           SOURCE_LINE_DLM_DATE_KEY 
		, HEADER_VERSION_NUMBER                              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER 
		, CASE WHEN nullif(array_to_string(array_construct_compact(INVOICE_TYPE,SUBROGATION_FLAG,ADJUSTMENT_TYPE,INPUT_METHOD_CODE,PAYMENT_CATEGORY,FEE_SCHEDULE,PAID_ABOVE_ZERO_IND
													,SUBROGATION_TYPE_DESC ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(INVOICE_TYPE as 
    varchar
), '') || '-' || coalesce(cast(SUBROGATION_FLAG as 
    varchar
), '') || '-' || coalesce(cast(ADJUSTMENT_TYPE as 
    varchar
), '') || '-' || coalesce(cast(INPUT_METHOD_CODE as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(FEE_SCHEDULE as 
    varchar
), '') || '-' || coalesce(cast(PAID_ABOVE_ZERO_IND as 
    varchar
), '') || '-' || coalesce(cast(SUBROGATION_TYPE_DESC as 
    varchar
), '')

 as 
    varchar
))                                     
                END                                          as                                  INVOICE_PROFILE_HKEY 
		, CASE WHEN RECEIPT_DATE is null then '-1' 
			WHEN RECEIPT_DATE < '1901-01-01' then '-2' 
			WHEN RECEIPT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( RECEIPT_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                          BWC_BILL_RECEIPT_DATE_KEY 
		, CASE WHEN NETWORK_RECEIPT_DATE is null then '-1' 
			WHEN NETWORK_RECEIPT_DATE < '1901-01-01' then '-2' 
			WHEN NETWORK_RECEIPT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( NETWORK_RECEIPT_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                           NETWORK_RECEIPT_DATE_KEY 
		, CASE WHEN SERVICE_FROM is null then '-1' 
			WHEN SERVICE_FROM < '1901-01-01' then '-2' 
			WHEN SERVICE_FROM > '2099-12-31' then '-3' 
			ELSE regexp_replace( SERVICE_FROM, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                      INVOICE_SERVICE_FROM_DATE_KEY 
		, CASE WHEN SERVICE_TO is null then '-1' 
			WHEN SERVICE_TO < '1901-01-01' then '-2' 
			WHEN SERVICE_TO > '2099-12-31' then '-3' 
			ELSE regexp_replace( SERVICE_TO, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                        INVOICE_SERVICE_TO_DATE_KEY 
		, CASE WHEN DATE_OF_SERVICE_FROM is null then '-1' 
			WHEN DATE_OF_SERVICE_FROM < '1901-01-01' then '-2' 
			WHEN DATE_OF_SERVICE_FROM > '2099-12-31' then '-3' 
			ELSE regexp_replace( DATE_OF_SERVICE_FROM, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                         LINE_SERVICE_FROM_DATE_KEY 
		, CASE WHEN DATE_OF_SERVICE_TO is null then '-1' 
			WHEN DATE_OF_SERVICE_TO < '1901-01-01' then '-2' 
			WHEN DATE_OF_SERVICE_TO > '2099-12-31' then '-3' 
			ELSE regexp_replace( DATE_OF_SERVICE_TO, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                           LINE_SERVICE_TO_DATE_KEY 
		, DIAGNOSIS_SEQUENCE                                 as                                 DIAGNOSIS_SEQUENCE 
		, CASE WHEN LINE_STATUS_CODE  IS NULL 
				THEN MD5( '99999' ) ELSE 
				md5(cast(
    
    coalesce(cast(LINE_STATUS_CODE as 
    varchar
), '')

 as 
    varchar
)) END
		                                   as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, CASE WHEN INVOICE_STATUS IS NULL 
				THEN MD5( '99999' ) ELSE 
				md5(cast(
    
    coalesce(cast(INVOICE_STATUS as 
    varchar
), '')

 as 
    varchar
)) END 
		                                    as                 INVOICE_HEADER_CURRENT_STATUS_HKEY 
		, CASE WHEN INTEREST_ACCRUAL_DATE is null then '-1' 
			WHEN INTEREST_ACCRUAL_DATE < '1901-01-01' then '-2' 
			WHEN INTEREST_ACCRUAL_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INTEREST_ACCRUAL_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                          INTEREST_ACCRUAL_DATE_KEY 
		, CASE WHEN ADJUDICATION_STATUS_DATE is null then '-1' 
			WHEN ADJUDICATION_STATUS_DATE < '1901-01-01' then '-2' 
			WHEN ADJUDICATION_STATUS_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( ADJUDICATION_STATUS_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                          BWC_ADJUDICATION_DATE_KEY 
		, CASE WHEN PAYMENT_DATE is null then '-1' 
			WHEN PAYMENT_DATE < '1901-01-01' then '-2' 
			WHEN PAYMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PAYMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                             as                                      PAID_DATE_KEY 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD															 
		, CASE WHEN nullif(array_to_string(array_construct_compact(CLM_TYP_CD,CHNG_OVR_IND,CLM_STT_TYP_CD,CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD ),''), '') is NULL  
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
                END                                          as                                  CLAIM_TYPE_STATUS_HKEY 
       , CASE WHEN  nullif(array_to_string(array_construct_compact( OCCR_SRC_TYP_NM, OCCR_MEDA_TYP_NM, NOI_CTG_TYP_NM, NOI_TYP_NM, FIREFIGHTER_CANCER_IND, COVID_EXPOSURE_IND, COVID_EMERGENCY_WORKER_IND, COVID_HEALTH_CARE_WORKER_IND, COMBINED_CLAIM_IND, SB223_IND, EMPLOYER_PREMISES_IND, CLM_CTRPH_INJR_IND, K_PROGRAM_ENROLLMENT_DESC, K_PROGRAM_TYPE_DESC, K_PROGRAM_REASON_DESC ),''),'') is NULL 
			then MD5( '99999' ) ELSE 
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
				END                                          as                     CLAIM_DETAIL_HKEY
		-- , MD5( '99999' )                                     as               HEALTHCARE_AUTHORIZATION_STATUS_HKEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact(POLICY_TYPE_CODE,PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND ),''), '') is NULL 
				THEN MD5( '99999' ) ELSE 
		md5(cast(
    
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
				END                                          as                               POLICY_STANDING_HKEY 
		, CASE WHEN WRNT_DATE is null then '-1' 
			WHEN WRNT_DATE < '1901-01-01' then '-2' 
			WHEN WRNT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( WRNT_DATE, '[^0-9]+', '') 
				END :: INTEGER                               as                          ORIGINAL_WARRANT_DATE_KEY    
		, WRNT_NO											 as                            ORIGINAL_WARRANT_NUMBER 
		, SUBMITTED_POLICY_NUMBER                            as                    INVOICE_SUBMITTED_POLICY_NUMBER 
		, UNITS_OF_SERVICE                                   as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT 
		, PAID_UNIT_QUANTITY                                 as                   LINE_UNITS_OF_PAID_SERVICE_COUNT 
		, BILLED_AMOUNT                                      as                        LINE_PROVIDER_BILLED_AMOUNT 
		, NTWK_BILLED_AMT                                    as                            LINE_MCO_ALLOWED_AMOUNT 
		, FEE_SCHED_AMOUNT                                   as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT 
		, CALC_AMOUNT                                        as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT 
		, APPROVED_AMOUNT                                    as                           LINE_BWC_APPROVED_AMOUNT 
		, INTEREST_AMOUNT                                    as                               LINE_INTEREST_AMOUNT 
		, (nvl(APPROVED_AMOUNT,0) + nvl(INTEREST_AMOUNT,0))  as                             LINE_REIMBURSED_AMOUNT 
		, BATCH_NUMBER                                       as                              BUSINESS_BATCH_NUMBER 
		, INVOICE_TYPE_DESC                                  as                                  INVOICE_TYPE_DESC 
		, MCO_NUMBER                                         as                                 INVOICE_MCO_NUMBER 
		, SERVICING_PEACH_NUMBER                             as                     INVOICE_SERVICING_PEACH_NUMBER 
		, PAYTO_PEACH_NUMBER                                 as                         INVOICE_PAYTO_PEACH_NUMBER 
		, PROCEDURE_CODE                                     as                             INVOICE_PROCEDURE_CODE 
		, PLACE_OF_SERVICE_CODE                              as                      INVOICE_PLACE_OF_SERVICE_CODE 
		, REFERRING_PEACH_NUMBER                             as                     INVOICE_REFERRING_PEACH_NUMBER 
		, DIAGNOSIS_CODE                                     as                             INVOICE_DIAGNOSIS_CODE 
		, DIAGNOSIS1                                         as                                 INVOICE_DIAGNOSIS1 
		, DIAGNOSIS2                                         as                                 INVOICE_DIAGNOSIS2 
		, DIAGNOSIS3                                         as                                 INVOICE_DIAGNOSIS3 
		, DIAGNOSIS4                                         as                                 INVOICE_DIAGNOSIS4 
		, PAID_MCO_NUMBER                                    as                                   INVOICE_PAID_MCO 
		, CUSTOMER_NUMBER                                    as                            INVOICE_CUSTOMER_NUMBER 
		, INVOICE_TYPE                                       as                               INVOICE_INVOICE_TYPE 
		, SUBROGATION_FLAG                                   as                           INVOICE_SUBROGATION_FLAG 
		, ADJUSTMENT_TYPE                                    as                            INVOICE_ADJUSTMENT_TYPE 
		, INPUT_METHOD_CODE                                  as                          INVOICE_INPUT_METHOD_CODE 
		, PAYMENT_CATEGORY                                   as                           INVOICE_PAYMENT_CATEGORY 
		, FEE_SCHEDULE                                       as                               INVOICE_FEE_SCHEDULE 
		, MOD1_MODIFIER_CODE                                 as                         INVOICE_MOD1_MODIFIER_CODE 
		, MOD2_MODIFIER_CODE                                 as                         INVOICE_MOD2_MODIFIER_CODE 
		, MOD3_MODIFIER_CODE                                 as                         INVOICE_MOD3_MODIFIER_CODE 
		, MOD4_MODIFIER_CODE                                 as                         INVOICE_MOD4_MODIFIER_CODE 
		, MODIFER_TYPE                                       as                               INVOICE_MODIFER_TYPE 
		, RECEIPT_DATE                                       as                               INVOICE_RECEIPT_DATE 
		, SERVICE_FROM                                       as                        INVOICE_HEADER_SERVICE_FROM 
		, PAYMENT_DATE                                       as                               INVOICE_PAYMENT_DATE 
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, DATE_OF_SERVICE_FROM                               as                          LINE_DATE_OF_SERVICE_FROM 
		, CLAIM_NUMBER										 as 									  CLAIM_NUMBER

		from SRC_INVOICE
            ),
LOGIC_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                            SUBMITTING_NETWORK_HKEY 
		, NETWORK_NUMBER                                     as                                NTWK_NETWORK_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                         NTWK_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                               NTWK_RECORD_END_DATE  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_NTWK
            ),
LOGIC_S_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                              SERVICE_PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as                      S_PRVDR_PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                      S_PRVDR_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                            S_PRVDR_RECORD_END_DATE 
		from SRC_S_PRVDR
            ),
LOGIC_P_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                               PAY_TO_PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as                      P_PRVDR_PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                      P_PRVDR_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                            P_PRVDR_RECORD_END_DATE 
		from SRC_P_PRVDR
            ),
LOGIC_CPT as ( SELECT 
		  CPT_HKEY                                           as                          CPT_SERVICE_RENDERED_HKEY 
		, PROCEDURE_CODE                                     as                                 CPT_PROCEDURE_CODE 
		, RECORD_EFFECTIVE_DATE                              as                          CPT_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                CPT_RECORD_END_DATE 
		from SRC_CPT
            ),
LOGIC_PL_SVC as ( SELECT 
		  PLACE_OF_SERVICE_HKEY                              as                              PLACE_OF_SERVICE_HKEY 
		, LINE_PLACE_OF_SERVICE_CODE                         as                  PL_SVC_LINE_PLACE_OF_SERVICE_CODE 
		, RECORD_EFFECTIVE_DATE                              as                       PL_SVC_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                             PL_SVC_RECORD_END_DATE 
		from SRC_PL_SVC
            ),
LOGIC_R_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                            REFERRING_PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as                      R_PRVDR_PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                      R_PRVDR_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                            R_PRVDR_RECORD_END_DATE 
		from SRC_R_PRVDR
            ),
LOGIC_P_ICD as ( SELECT 
		  ICD_HKEY                                           as                                 PRINCIPAL_ICD_HKEY 
		, ICD_CODE                                           as                                     P_ICD_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                               P_ICD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                     P_ICD_END_DATE 
		, ICD_CODE_VERSION_NUMBER                            as                      P_ICD_ICD_CODE_VERSION_NUMBER 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_P_ICD
            ),
LOGIC_ICD1 as ( SELECT 
		  ICD_HKEY                                           as                                         ICD_1_HKEY 
		, ICD_CODE                                           as                                      ICD1_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD1_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD1_END_DATE 
		, ICD_CODE_VERSION_NUMBER                            as                       ICD1_ICD_CODE_VERSION_NUMBER  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_ICD1
            ),
LOGIC_ICD2 as ( SELECT 
		  ICD_HKEY                                           as                                         ICD_2_HKEY 
		, ICD_CODE                                           as                                      ICD2_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD2_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD2_END_DATE 
		, ICD_CODE_VERSION_NUMBER                            as                       ICD2_ICD_CODE_VERSION_NUMBER  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_ICD2
            ),
LOGIC_ICD3 as ( SELECT 
		  ICD_HKEY                                           as                                         ICD_3_HKEY 
		, ICD_CODE                                           as                                      ICD3_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD3_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD3_END_DATE 
		, ICD_CODE_VERSION_NUMBER                            as                       ICD3_ICD_CODE_VERSION_NUMBER 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_ICD3
            ),
LOGIC_ICD4 as ( SELECT 
		  ICD_HKEY                                           as                                         ICD_4_HKEY 
		, ICD_CODE                                           as                                      ICD4_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD4_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD4_END_DATE 
		, ICD_CODE_VERSION_NUMBER                            as                       ICD4_ICD_CODE_VERSION_NUMBER  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_ICD4
            ),
LOGIC_P_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                PAY_TO_NETWORK_HKEY 
		, NETWORK_NUMBER                                     as                              P_NTWK_NETWORK_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                       P_NTWK_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                             P_NTWK_RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_P_NTWK
            ),
LOGIC_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    as                       IW_CORESUITE_CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                           IW_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                 IW_RECORD_END_DATE 
		from SRC_IW
            )--,
/*LOGIC_CLM as ( SELECT 
		  CLAIM_HKEY                                         as                                  CLAIM_NUMBER_HKEY 
		, CLAIM_NUMBER										 as 									  CLAIM_NUMBER
		from SRC_CLM
            ) */

---- RENAME LAYER ----
,

RENAME_INVOICE as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY
		, DIAGNOSIS_SEQUENCE                                 as                                 DIAGNOSIS_SEQUENCE
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD			
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		-- , HEALTHCARE_AUTHORIZATION_STATUS_HKEY               as               HEALTHCARE_AUTHORIZATION_STATUS_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, CLAIM_NUMBER                                       as                                   CLAIM_NUMBER
        , LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_MCO_ALLOWED_AMOUNT                            as                            LINE_MCO_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER
		, INVOICE_TYPE_DESC                                  as                                  INVOICE_TYPE_DESC
		, INVOICE_MCO_NUMBER                                 as                                 INVOICE_MCO_NUMBER
		, INVOICE_SERVICING_PEACH_NUMBER                     as                     INVOICE_SERVICING_PEACH_NUMBER
		, INVOICE_PAYTO_PEACH_NUMBER                         as                         INVOICE_PAYTO_PEACH_NUMBER
		, INVOICE_PROCEDURE_CODE                             as                             INVOICE_PROCEDURE_CODE
		, INVOICE_PLACE_OF_SERVICE_CODE                      as                      INVOICE_PLACE_OF_SERVICE_CODE
		, INVOICE_REFERRING_PEACH_NUMBER                     as                     INVOICE_REFERRING_PEACH_NUMBER
		, INVOICE_DIAGNOSIS_CODE                             as                             INVOICE_DIAGNOSIS_CODE
		, INVOICE_DIAGNOSIS1                                 as                                 INVOICE_DIAGNOSIS1
		, INVOICE_DIAGNOSIS2                                 as                                 INVOICE_DIAGNOSIS2
		, INVOICE_DIAGNOSIS3                                 as                                 INVOICE_DIAGNOSIS3
		, INVOICE_DIAGNOSIS4                                 as                                 INVOICE_DIAGNOSIS4
		, INVOICE_PAID_MCO                                   as                                   INVOICE_PAID_MCO
		, INVOICE_CUSTOMER_NUMBER                            as                            INVOICE_CUSTOMER_NUMBER
		, INVOICE_INVOICE_TYPE                               as                               INVOICE_INVOICE_TYPE
		, INVOICE_SUBROGATION_FLAG                           as                           INVOICE_SUBROGATION_FLAG
		, INVOICE_ADJUSTMENT_TYPE                            as                            INVOICE_ADJUSTMENT_TYPE
		, INVOICE_INPUT_METHOD_CODE                          as                          INVOICE_INPUT_METHOD_CODE
		, INVOICE_PAYMENT_CATEGORY                           as                           INVOICE_PAYMENT_CATEGORY
		, INVOICE_FEE_SCHEDULE                               as                               INVOICE_FEE_SCHEDULE
		, INVOICE_MOD1_MODIFIER_CODE                         as                         INVOICE_MOD1_MODIFIER_CODE
		, INVOICE_MOD2_MODIFIER_CODE                         as                         INVOICE_MOD2_MODIFIER_CODE
		, INVOICE_MOD3_MODIFIER_CODE                         as                         INVOICE_MOD3_MODIFIER_CODE
		, INVOICE_MOD4_MODIFIER_CODE                         as                         INVOICE_MOD4_MODIFIER_CODE
		, INVOICE_MODIFER_TYPE                               as                               INVOICE_MODIFER_TYPE
		, INVOICE_RECEIPT_DATE                               as                               INVOICE_RECEIPT_DATE
		, INVOICE_HEADER_SERVICE_FROM                        as                        INVOICE_HEADER_SERVICE_FROM
		, INVOICE_PAYMENT_DATE                               as                               INVOICE_PAYMENT_DATE
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, LINE_DATE_OF_SERVICE_FROM                          as                          LINE_DATE_OF_SERVICE_FROM 
	
				FROM     LOGIC_INVOICE   ), 
RENAME_NTWK as ( SELECT 
		  SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY
		, NTWK_NETWORK_NUMBER                                as                                NTWK_NETWORK_NUMBER
		, NTWK_RECORD_EFFECTIVE_DATE                         as                         NTWK_RECORD_EFFECTIVE_DATE
		, NTWK_RECORD_END_DATE                               as                               NTWK_RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                            NTWK_CURRENT_RECORD_IND 
				FROM     LOGIC_NTWK   ), 
RENAME_S_PRVDR as ( SELECT 
		  SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY
		, S_PRVDR_PROVIDER_PEACH_NUMBER                      as                      S_PRVDR_PROVIDER_PEACH_NUMBER
		, S_PRVDR_RECORD_EFFECTIVE_DATE                      as                      S_PRVDR_RECORD_EFFECTIVE_DATE
		, S_PRVDR_RECORD_END_DATE                            as                            S_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_S_PRVDR   ), 
RENAME_P_PRVDR as ( SELECT 
		  PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY
		, P_PRVDR_PROVIDER_PEACH_NUMBER                      as                      P_PRVDR_PROVIDER_PEACH_NUMBER
		, P_PRVDR_RECORD_EFFECTIVE_DATE                      as                      P_PRVDR_RECORD_EFFECTIVE_DATE
		, P_PRVDR_RECORD_END_DATE                            as                            P_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_P_PRVDR   ), 
RENAME_CPT as ( SELECT 
		  CPT_SERVICE_RENDERED_HKEY                          as                          CPT_SERVICE_RENDERED_HKEY
		, CPT_PROCEDURE_CODE                                 as                                 CPT_PROCEDURE_CODE
		, CPT_RECORD_EFFECTIVE_DATE                          as                          CPT_RECORD_EFFECTIVE_DATE
		, CPT_RECORD_END_DATE                                as                                CPT_RECORD_END_DATE 
				FROM     LOGIC_CPT   ), 
RENAME_PL_SVC as ( SELECT 
		  PLACE_OF_SERVICE_HKEY                              as                              PLACE_OF_SERVICE_HKEY
		, PL_SVC_LINE_PLACE_OF_SERVICE_CODE                  as                  PL_SVC_LINE_PLACE_OF_SERVICE_CODE
		, PL_SVC_RECORD_EFFECTIVE_DATE                       as                       PL_SVC_RECORD_EFFECTIVE_DATE
		, PL_SVC_RECORD_END_DATE                             as                             PL_SVC_RECORD_END_DATE 
				FROM     LOGIC_PL_SVC   ), 
RENAME_R_PRVDR as ( SELECT 
		  REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY
		, R_PRVDR_PROVIDER_PEACH_NUMBER                      as                      R_PRVDR_PROVIDER_PEACH_NUMBER
		, R_PRVDR_RECORD_EFFECTIVE_DATE                      as                      R_PRVDR_RECORD_EFFECTIVE_DATE
		, R_PRVDR_RECORD_END_DATE                            as                            R_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_R_PRVDR   ), 
RENAME_P_ICD as ( SELECT 
		  PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
		, P_ICD_ICD_CODE                                     as                                     P_ICD_ICD_CODE
		, P_ICD_EFFECTIVE_DATE                               as                               P_ICD_EFFECTIVE_DATE
		, P_ICD_END_DATE                                     as                                     P_ICD_END_DATE
		, P_ICD_ICD_CODE_VERSION_NUMBER                      as                      P_ICD_ICD_CODE_VERSION_NUMBER
		, CURRENT_RECORD_IND                                 as                           P_ICD_CURRENT_RECORD_IND  
				FROM     LOGIC_P_ICD   ), 
RENAME_ICD1 as ( SELECT 
		  ICD_1_HKEY                                         as                                         ICD_1_HKEY
		, ICD1_ICD_CODE                                      as                                      ICD1_ICD_CODE
		, ICD1_EFFECTIVE_DATE                                as                                ICD1_EFFECTIVE_DATE
		, ICD1_END_DATE                                      as                                      ICD1_END_DATE
		, ICD1_ICD_CODE_VERSION_NUMBER                       as                       ICD1_ICD_CODE_VERSION_NUMBER
		, CURRENT_RECORD_IND                                 as                             ICD1_CURRENT_RECORD_IND 
				FROM     LOGIC_ICD1   ), 
RENAME_ICD2 as ( SELECT 
		  ICD_2_HKEY                                         as                                         ICD_2_HKEY
		, ICD2_ICD_CODE                                      as                                      ICD2_ICD_CODE
		, ICD2_EFFECTIVE_DATE                                as                                ICD2_EFFECTIVE_DATE
		, ICD2_END_DATE                                      as                                      ICD2_END_DATE
		, ICD2_ICD_CODE_VERSION_NUMBER                       as                       ICD2_ICD_CODE_VERSION_NUMBER 
		, CURRENT_RECORD_IND                                 as                            ICD2_CURRENT_RECORD_IND
				FROM     LOGIC_ICD2   ), 
RENAME_ICD3 as ( SELECT 
		  ICD_3_HKEY                                         as                                         ICD_3_HKEY
		, ICD3_ICD_CODE                                      as                                      ICD3_ICD_CODE
		, ICD3_EFFECTIVE_DATE                                as                                ICD3_EFFECTIVE_DATE
		, ICD3_END_DATE                                      as                                      ICD3_END_DATE
		, ICD3_ICD_CODE_VERSION_NUMBER                       as                       ICD3_ICD_CODE_VERSION_NUMBER  
		, CURRENT_RECORD_IND                                 as                            ICD3_CURRENT_RECORD_IND
				FROM     LOGIC_ICD3   ), 
RENAME_ICD4 as ( SELECT 
		  ICD_4_HKEY                                         as                                         ICD_4_HKEY
		, ICD4_ICD_CODE                                      as                                      ICD4_ICD_CODE
		, ICD4_EFFECTIVE_DATE                                as                                ICD4_EFFECTIVE_DATE
		, ICD4_END_DATE                                      as                                      ICD4_END_DATE
		, ICD4_ICD_CODE_VERSION_NUMBER                       as                       ICD4_ICD_CODE_VERSION_NUMBER
		, CURRENT_RECORD_IND                                 as                            ICD4_CURRENT_RECORD_IND 
				FROM     LOGIC_ICD4   ), 
RENAME_P_NTWK as ( SELECT 
		  PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY
		, P_NTWK_NETWORK_NUMBER                              as                              P_NTWK_NETWORK_NUMBER
		, P_NTWK_RECORD_EFFECTIVE_DATE                       as                       P_NTWK_RECORD_EFFECTIVE_DATE
		, P_NTWK_RECORD_END_DATE                             as                             P_NTWK_RECORD_END_DATE  
		, CURRENT_RECORD_IND                                 as                          P_NTWK_CURRENT_RECORD_IND
				FROM     LOGIC_P_NTWK   ), 
RENAME_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
		, IW_CORESUITE_CUSTOMER_NUMBER                       as                       IW_CORESUITE_CUSTOMER_NUMBER
		, IW_RECORD_EFFECTIVE_DATE                           as                           IW_RECORD_EFFECTIVE_DATE
		, IW_RECORD_END_DATE                                 as                                 IW_RECORD_END_DATE 
				FROM     LOGIC_IW   )--, 
/*RENAME_CLM as ( SELECT 
          CLAIM_NUMBER										 as 								   CLM_CLAIM_NUMBER
				FROM     LOGIC_CLM   )*/

---- FILTER LAYER (uses aliases) ----
,
FILTER_INVOICE                        as ( SELECT * from    RENAME_INVOICE 
                                            WHERE INVOICE_TYPE_DESC in ('PROFESSIONAL', 'ASC')  ),
FILTER_NTWK                           as ( SELECT * from    RENAME_NTWK   ),
FILTER_S_PRVDR                        as ( SELECT * from    RENAME_S_PRVDR   ),
FILTER_P_PRVDR                        as ( SELECT * from    RENAME_P_PRVDR   ),
FILTER_CPT                            as ( SELECT * from    RENAME_CPT   ),
FILTER_PL_SVC                         as ( SELECT * from    RENAME_PL_SVC   ),
FILTER_R_PRVDR                        as ( SELECT * from    RENAME_R_PRVDR   ),
FILTER_P_ICD                          as ( SELECT * from    RENAME_P_ICD   ),
FILTER_ICD1                           as ( SELECT * from    RENAME_ICD1   ),
FILTER_ICD2                           as ( SELECT * from    RENAME_ICD2   ),
FILTER_ICD3                           as ( SELECT * from    RENAME_ICD3   ),
FILTER_ICD4                           as ( SELECT * from    RENAME_ICD4   ),
FILTER_P_NTWK                         as ( SELECT * from    RENAME_P_NTWK   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
--FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),

---- JOIN LAYER ----

INVOICE as ( SELECT * 
				FROM  FILTER_INVOICE
				LEFT JOIN FILTER_S_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_SERVICING_PEACH_NUMBER, '99999999999') =  FILTER_S_PRVDR.S_PRVDR_PROVIDER_PEACH_NUMBER AND COALESCE(LINE_DATE_OF_SERVICE_FROM, INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN S_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( S_PRVDR_RECORD_END_DATE, '2099-12-31') 
						LEFT JOIN FILTER_NTWK ON  coalesce( FILTER_INVOICE.INVOICE_MCO_NUMBER, '00000') =  FILTER_NTWK.NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN NTWK_RECORD_EFFECTIVE_DATE AND coalesce( NTWK_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_PAYTO_PEACH_NUMBER, '99999999999') =  FILTER_P_PRVDR.P_PRVDR_PROVIDER_PEACH_NUMBER AND COALESCE(LINE_DATE_OF_SERVICE_FROM, INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN P_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( P_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_CPT ON  coalesce( FILTER_INVOICE.INVOICE_PROCEDURE_CODE, 'UNK') =  FILTER_CPT.CPT_PROCEDURE_CODE AND COALESCE(LINE_DATE_OF_SERVICE_FROM, INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN CPT_RECORD_EFFECTIVE_DATE AND coalesce( CPT_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_PL_SVC ON  coalesce( FILTER_INVOICE.INVOICE_PLACE_OF_SERVICE_CODE, 'UNK') =  FILTER_PL_SVC.PL_SVC_LINE_PLACE_OF_SERVICE_CODE AND COALESCE(INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN PL_SVC_RECORD_EFFECTIVE_DATE AND coalesce( PL_SVC_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_R_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_REFERRING_PEACH_NUMBER, '99999999999') =  FILTER_R_PRVDR.R_PRVDR_PROVIDER_PEACH_NUMBER AND COALESCE(LINE_DATE_OF_SERVICE_FROM, INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN R_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( R_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_ICD ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS_CODE,  'UNK') =  FILTER_P_ICD.P_ICD_ICD_CODE AND COALESCE(INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN P_ICD_EFFECTIVE_DATE AND coalesce( P_ICD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD1 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS1,  'UNK') =  FILTER_ICD1.ICD1_ICD_CODE AND COALESCE(LINE_DATE_OF_SERVICE_FROM, '1901-01-01') BETWEEN ICD1_EFFECTIVE_DATE AND coalesce( ICD1_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD2 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS2,  'UNK') =  FILTER_ICD2.ICD2_ICD_CODE AND COALESCE(LINE_DATE_OF_SERVICE_FROM, '1901-01-01') BETWEEN ICD2_EFFECTIVE_DATE AND coalesce( ICD2_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD3 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS3,  'UNK') =  FILTER_ICD3.ICD3_ICD_CODE AND COALESCE(LINE_DATE_OF_SERVICE_FROM, '1901-01-01') BETWEEN ICD3_EFFECTIVE_DATE AND coalesce( ICD3_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD4 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS4,  'UNK') =  FILTER_ICD4.ICD4_ICD_CODE AND COALESCE(LINE_DATE_OF_SERVICE_FROM, '1901-01-01') BETWEEN ICD4_EFFECTIVE_DATE AND coalesce( ICD4_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_NTWK ON  coalesce( FILTER_INVOICE.INVOICE_PAID_MCO, '00000') =  FILTER_P_NTWK.P_NTWK_NETWORK_NUMBER AND coalesce(INVOICE_PAYMENT_DATE, '1901-01-01') BETWEEN P_NTWK_RECORD_EFFECTIVE_DATE AND coalesce( P_NTWK_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_IW ON  coalesce( FILTER_INVOICE.INVOICE_CUSTOMER_NUMBER, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND COALESCE(LINE_DATE_OF_SERVICE_FROM, INVOICE_HEADER_SERVICE_FROM, '1901-01-01') BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31') )
								--LEFT JOIN FILTER_CLM ON  coalesce( FILTER_INVOICE.CLAIM_NUMBER, 'UNK') =  FILTER_CLM.CLM_CLAIM_NUMBER  )
-- ETL join layer to handle NDC & ICDs that are outside of date range MD5('-2222') 
								                                
, ETL_SRT AS (SELECT FILTER_INVOICE.*
            , FILTER_P_ICD1.P_ICD_ICD_CODE AS FILTER_P_ICD1_P_ICD_ICD_CODE
            , FILTER_ICD1_SRT.ICD1_ICD_CODE AS FILTER_ICD1_SRT_ICD1_ICD_CODE
            , FILTER_ICD2_SRT.ICD2_ICD_CODE AS FILTER_ICD2_SRT_ICD2_ICD_CODE
            , FILTER_ICD3_SRT.ICD3_ICD_CODE AS FILTER_ICD3_SRT_ICD3_ICD_CODE
            , FILTER_ICD4_SRT.ICD4_ICD_CODE AS FILTER_ICD4_SRT_ICD4_ICD_CODE
			, FILTER_NTWK1.NTWK_NETWORK_NUMBER AS FILTER_NTWK1_NTWK_NETWORK_NUMBER
			, FILTER_P_NTWK1.P_NTWK_NETWORK_NUMBER AS FILTER_P_NTWK1_P_NTWK_NETWORK_NUMBER
            FROM INVOICE FILTER_INVOICE
            LEFT JOIN FILTER_P_ICD FILTER_P_ICD1 ON   coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS_CODE,  'Z') =  FILTER_P_ICD1.P_ICD_ICD_CODE AND FILTER_P_ICD1.P_ICD_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD1 FILTER_ICD1_SRT ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS1,  'Z') =  FILTER_ICD1_SRT.ICD1_ICD_CODE AND FILTER_ICD1_SRT.ICD1_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD2 FILTER_ICD2_SRT ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS2,  'Z') =  FILTER_ICD2_SRT.ICD2_ICD_CODE AND FILTER_ICD2_SRT.ICD2_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD3 FILTER_ICD3_SRT ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS3,  'Z') =  FILTER_ICD3_SRT.ICD3_ICD_CODE AND FILTER_ICD3_SRT.ICD3_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD4 FILTER_ICD4_SRT ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS4,  'Z') =  FILTER_ICD4_SRT.ICD4_ICD_CODE AND FILTER_ICD4_SRT.ICD4_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_NTWK FILTER_NTWK1 ON     coalesce( FILTER_INVOICE.INVOICE_MCO_NUMBER, 'Z') =  FILTER_NTWK1.NTWK_NETWORK_NUMBER AND FILTER_NTWK1.NTWK_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_P_NTWK FILTER_P_NTWK1 ON coalesce( FILTER_INVOICE.INVOICE_PAID_MCO, 'Z') =  FILTER_P_NTWK1.P_NTWK_NETWORK_NUMBER AND FILTER_P_NTWK1.P_NTWK_CURRENT_RECORD_IND = 'Y' 
			)

SELECT 
		  MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, coalesce( INVOICE_PROFILE_HKEY, MD5( '-1111' )) as INVOICE_PROFILE_HKEY
		, CASE WHEN SUBMITTING_NETWORK_HKEY IS NOT NULL THEN SUBMITTING_NETWORK_HKEY
                WHEN FILTER_NTWK1_NTWK_NETWORK_NUMBER IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS SUBMITTING_NETWORK_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY
		, coalesce( SERVICE_PROVIDER_HKEY, MD5( '-1111' )) as SERVICE_PROVIDER_HKEY
		, coalesce( PAY_TO_PROVIDER_HKEY, MD5( '-1111' )) as PAY_TO_PROVIDER_HKEY
		, coalesce( CPT_SERVICE_RENDERED_HKEY, MD5( '-1111' )) as CPT_SERVICE_RENDERED_HKEY
		, CASE WHEN nullif(array_to_string(array_construct_compact(INVOICE_MOD1_MODIFIER_CODE,INVOICE_MOD2_MODIFIER_CODE,INVOICE_MOD3_MODIFIER_CODE,INVOICE_MOD4_MODIFIER_CODE
													,INVOICE_MODIFER_TYPE ),''), '') is NULL  
				THEN MD5( '99999' ) ELSE 
		 md5(cast(
    
    coalesce(cast(INVOICE_MOD1_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_MOD2_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_MOD3_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_MOD4_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(INVOICE_MODIFER_TYPE as 
    varchar
), '')

 as 
    varchar
)) END
				 as                             MODIFIER_SEQUENCE_HKEY 
		, coalesce( PLACE_OF_SERVICE_HKEY, MD5( '-1111' )) as PLACE_OF_SERVICE_HKEY
		, INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY
		, coalesce( REFERRING_PROVIDER_HKEY, MD5( '-1111' )) as REFERRING_PROVIDER_HKEY
		, CASE WHEN P_ICD_ICD_CODE IS NOT NULL THEN PRINCIPAL_ICD_HKEY
                WHEN FILTER_P_ICD1_P_ICD_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PRINCIPAL_ICD_HKEY
		, DIAGNOSIS_SEQUENCE
		, CASE WHEN ICD1_ICD_CODE IS NOT NULL THEN ICD_1_HKEY
                WHEN FILTER_ICD1_SRT_ICD1_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS ICD_1_HKEY
		, CASE WHEN ICD2_ICD_CODE IS NOT NULL THEN ICD_2_HKEY
                WHEN FILTER_ICD2_SRT_ICD2_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS ICD_2_HKEY
		, CASE WHEN ICD3_ICD_CODE IS NOT NULL THEN ICD_3_HKEY
                WHEN FILTER_ICD3_SRT_ICD3_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS ICD_3_HKEY
		, CASE WHEN ICD4_ICD_CODE IS NOT NULL THEN ICD_4_HKEY
                WHEN FILTER_ICD4_SRT_ICD4_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS ICD_4_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY
		, CASE WHEN PAY_TO_NETWORK_HKEY IS NOT NULL THEN PAY_TO_NETWORK_HKEY
                WHEN FILTER_P_NTWK1_P_NTWK_NETWORK_NUMBER IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PAY_TO_NETWORK_HKEY
		, INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY
		, coalesce( INJURED_WORKER_HKEY, MD5( '-1111' )) as INJURED_WORKER_HKEY
		--, coalesce( CLAIM_NUMBER_HKEY, MD5( '-1111' )) as CLAIM_NUMBER_HKEY
		, coalesce( CLAIM_TYPE_STATUS_HKEY, MD5( '-1111' )) as CLAIM_TYPE_STATUS_HKEY
		, coalesce( CLAIM_DETAIL_HKEY, MD5( '-1111' )) as CLAIM_DETAIL_HKEY
		-- , HEALTHCARE_AUTHORIZATION_STATUS_HKEY
		, coalesce( POLICY_STANDING_HKEY, MD5( '-1111' )) as POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER
		, coalesce( CLAIM_NUMBER, MD5( '-1111' )) as CLAIM_NUMBER
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT
		, LINE_MCO_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT
		, CURRENT_TIMESTAMP() AS LOAD_DATETIME
		, 'CAM' AS PRIMARY_SOURCE_SYSTEM
		, BUSINESS_BATCH_NUMBER 
from ETL_SRT
      );
    