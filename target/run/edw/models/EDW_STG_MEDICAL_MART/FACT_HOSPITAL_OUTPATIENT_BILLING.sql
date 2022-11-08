

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FACT_HOSPITAL_OUTPATIENT_BILLING  as
      (

---- SRC LAYER ----
WITH
SRC_INVOICE        as ( SELECT *     FROM     STAGING.DSV_INVOICE_HOSPITAL ),
SRC_RC             as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_REVENUE_CENTER ),
SRC_APC            as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_AMBULATORY_PAYMENT_CLASSIFICATION ),
SRC_NTWK           as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_S_PRVDR        as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_P_PRVDR        as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_CPT            as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_CPT ),
SRC_A_ICD          as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_ICD          as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_NTWK         as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_IW             as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
SRC_CLM            as ( SELECT *     FROM     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
//SRC_INVOICE        as ( SELECT *     FROM     DSV_INVOICE_HOSPITAL) ,
//SRC_RC             as ( SELECT *     FROM     DIM_REVENUE_CENTER) ,
//SRC_APC            as ( SELECT *     FROM     DIM_AMBULATORY_PAYMENT_CLASSIFICATION) ,
//SRC_NTWK           as ( SELECT *     FROM     DIM_NETWORK) ,
//SRC_S_PRVDR        as ( SELECT *     FROM     DIM_PROVIDER) ,
//SRC_P_PRVDR        as ( SELECT *     FROM     DIM_PROVIDER) ,
//SRC_CPT            as ( SELECT *     FROM     DIM_CPT) ,
//SRC_A_ICD          as ( SELECT *     FROM     DIM_ICD) ,
//SRC_P_ICD          as ( SELECT *     FROM     DIM_ICD) ,
//SRC_P_NTWK         as ( SELECT *     FROM     DIM_NETWORK) ,
//SRC_IW             as ( SELECT *     FROM     DIM_INJURED_WORKER) ,
//SRC_CLM            as ( SELECT *     FROM     DIM_INJURED_WORKER) ,

---- LOGIC LAYER ----

LOGIC_INVOICE as ( SELECT 
		  INVOICE_NUMBER                                     AS                                     INVOICE_NUMBER 
		, LINE_SEQUENCE                                      AS                                      LINE_SEQUENCE 
		, SEQUENCE_EXTENSION                                 AS                                 SEQUENCE_EXTENSION 
		, LINE_VERSION_NUMBER                                AS                                LINE_VERSION_NUMBER 
		, CASE WHEN LINE_DLM IS NULL THEN -1
				WHEN REPLACE(CAST(LINE_DLM::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(LINE_DLM::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(CAST(LINE_DLM::DATE AS VARCHAR),'-','')::INTEGER 
				END                                          AS                                       LINE_DLM_KEY 
		, HEADER_VERSION_NUMBER                              AS                              HEADER_VERSION_NUMBER 
		, CASE WHEN nullif(array_to_string(array_construct_compact( ADMISSION_TYPE, ADMISSION_SOURCE, DISCHARGE_STATUS, BILL_TYPE),''), '') is NULL  
				THEN MD5( '99999' ) ELSE  
				md5(cast(
    
    coalesce(cast(ADMISSION_TYPE as 
    varchar
), '') || '-' || coalesce(cast(ADMISSION_SOURCE as 
    varchar
), '') || '-' || coalesce(cast(DISCHARGE_STATUS as 
    varchar
), '') || '-' || coalesce(cast(BILL_TYPE as 
    varchar
), '')

 as 
    varchar
)) 
				END				                             AS                                     ADMISSION_HKEY       
		, CASE WHEN ADMISSION_DATE IS NULL THEN -1
				WHEN REPLACE(CAST(ADMISSION_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(ADMISSION_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(CAST(ADMISSION_DATE::DATE AS VARCHAR),'-','')::INTEGER 
				END							                 AS                                 ADMISSION_DATE_KEY   
		, CASE WHEN DISCHARGE_DATE IS NULL THEN -1
				WHEN REPLACE(CAST(DISCHARGE_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(DISCHARGE_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(CAST(DISCHARGE_DATE::DATE AS VARCHAR),'-','')::INTEGER 
				END	                         				 AS                                 DISCHARGE_DATE_KEY   
		, CASE WHEN nullif(array_to_string(array_construct_compact( APC_STATUS, NON_OPPS_BILL_TYPE, OPPS_FLAG, PAYMENT_METHOD
							, DISCOUNT, COMPOSITE_ADJUSTMENT, PAYMENT_INDICATOR, PACKAGING, PAYMENT_ADJUSTMENT),''), '') is NULL  
				THEN MD5( '99999' ) ELSE
			    md5(cast(
    
    coalesce(cast(APC_STATUS as 
    varchar
), '') || '-' || coalesce(cast(NON_OPPS_BILL_TYPE as 
    varchar
), '') || '-' || coalesce(cast(OPPS_FLAG as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_METHOD as 
    varchar
), '') || '-' || coalesce(cast(DISCOUNT as 
    varchar
), '') || '-' || coalesce(cast(COMPOSITE_ADJUSTMENT as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_INDICATOR as 
    varchar
), '') || '-' || coalesce(cast(PACKAGING as 
    varchar
), '') || '-' || coalesce(cast(PAYMENT_ADJUSTMENT as 
    varchar
), '')

 as 
    varchar
))  END
								                             AS                                       GROUPER_HKEY         
		, CASE WHEN nullif(array_to_string(array_construct_compact( INVOICE_TYPE,SUBROGATION_FLAG,ADJUSTMENT_TYPE,INPUT_METHOD_CODE,PAYMENT_CATEGORY,FEE_SCHEDULE),''), '') is NULL  
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
			END                                              AS                               INVOICE_PROFILE_HKEY 
		, RECEIPT_DATE                                       AS                                       RECEIPT_DATE 
		, CASE WHEN RECEIPT_DATE IS NULL THEN -1	
				WHEN REPLACE(CAST(RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(RECEIPT_DATE::VARCHAR,'-','')::INTEGER 
				END 										 AS 						 BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE                               AS                               NETWORK_RECEIPT_DATE 
		, CASE WHEN NETWORK_RECEIPT_DATE IS NULL THEN -1
			WHEN REPLACE(CAST(NETWORK_RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
			WHEN REPLACE(CAST(NETWORK_RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3	
			ELSE REPLACE(NETWORK_RECEIPT_DATE::VARCHAR,'-','')::INTEGER END 	AS 		  NETWORK_RECEIPT_DATE_KEY
		, CASE WHEN nullif(array_to_string(array_construct_compact( MOD1_MODIFIER_CODE,MOD2_MODIFIER_CODE,MOD3_MODIFIER_CODE,MOD4_MODIFIER_CODE,MOD_SET),''), '') is NULL  
				THEN MD5( '99999' ) ELSE
				md5(cast(
    
    coalesce(cast(MOD1_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(MOD2_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(MOD3_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(MOD4_MODIFIER_CODE as 
    varchar
), '') || '-' || coalesce(cast(MOD_SET as 
    varchar
), '')

 as 
    varchar
)) 
			END 											 AS 							MODIFIER_SEQUENCE_HKEY
		, CASE WHEN INVOICE_SERVICE_FROM_DATE IS NULL THEN -1
			WHEN REPLACE(CAST(INVOICE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
			WHEN REPLACE(CAST(INVOICE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
			ELSE REPLACE(INVOICE_SERVICE_FROM_DATE::VARCHAR,'-','')::INTEGER 
			END	AS INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_FROM_DATE                          AS                          INVOICE_SERVICE_FROM_DATE 
		, CASE WHEN INVOICE_SERVICE_TO_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(INVOICE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(INVOICE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(INVOICE_SERVICE_TO_DATE::VARCHAR,'-','')::INTEGER 
				END AS INVOICE_SERVICE_TO_DATE_KEY
		, INVOICE_SERVICE_TO_DATE                            AS                            INVOICE_SERVICE_TO_DATE 
		, CASE WHEN  LINE_SERVICE_FROM_DATE IS NULL THEN -1
				WHEN REPLACE(CAST( LINE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST( LINE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE( LINE_SERVICE_FROM_DATE::VARCHAR,'-','')::INTEGER 
				END	AS LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_FROM_DATE                             AS                             LINE_SERVICE_FROM_DATE 
		, CASE WHEN  LINE_SERVICE_TO_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST( LINE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST( LINE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE( LINE_SERVICE_TO_DATE::VARCHAR,'-','')::INTEGER 
				END AS LINE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_TO_DATE                               AS                               LINE_SERVICE_TO_DATE 
		, LINE_STATUS_CODE                                   AS                                   LINE_STATUS_CODE
		, CASE WHEN LINE_STATUS_CODE IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
				ELSE md5(cast(
    
    coalesce(cast(LINE_STATUS_CODE as 
    varchar
), '')

 as 
    varchar
)) END AS LINE_STATUS_CODE_HKEY 
		, INVOICE_STATUS                                     AS                                     INVOICE_STATUS 
		, CASE WHEN INVOICE_STATUS IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
				ELSE md5(cast(
    
    coalesce(cast(INVOICE_STATUS as 
    varchar
), '')

 as 
    varchar
)) END AS INVOICE_STATUS_HKEY
		, INTEREST_ACCRUAL_DATE                              AS                              INTEREST_ACCRUAL_DATE 
		, CASE WHEN INTEREST_ACCRUAL_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(INTEREST_ACCRUAL_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(INTEREST_ACCRUAL_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(INTEREST_ACCRUAL_DATE::VARCHAR,'-','')::INTEGER END AS INTEREST_ACCRUAL_DATE_KEY
		, ADJUDICATION_STATUS_DATE                           AS                           ADJUDICATION_STATUS_DATE 
		, CASE WHEN ADJUDICATION_STATUS_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(ADJUDICATION_STATUS_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(ADJUDICATION_STATUS_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(ADJUDICATION_STATUS_DATE::VARCHAR,'-','')::INTEGER END AS ADJUDICATION_STATUS_DATE_KEY
		, PAYMENT_DATE                                       AS                                       PAYMENT_DATE
		, CASE WHEN PAYMENT_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(PAYMENT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(PAYMENT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(PAYMENT_DATE::VARCHAR,'-','')::INTEGER END AS PAYMENT_DATE_KEY
		, CASE WHEN nullif(array_to_string(array_construct_compact( OCCR_SRC_TYP_NM,OCCR_MEDA_TYP_NM,NOI_CTG_TYP_NM,NOI_TYP_NM,FIREFIGHTER_CANCER_IND,COVID_EXPOSURE_IND,COVID_EMERGENCY_WORKER_IND,COVID_HEALTH_CARE_WORKER_IND,COMBINED_CLAIM_IND,SB223_IND,EMPLOYER_PREMISES_IND,CLM_CTRPH_INJR_IND,K_PROGRAM_ENROLLMENT_DESC,K_PROGRAM_TYPE_DESC,K_PROGRAM_REASON_DESC),''), '') is NULL  
				THEN MD5( '99999' ) ELSE
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
			END 											 AS 							CLAIM_DETAIL_HKEY		 
		, CASE WHEN nullif(array_to_string(array_construct_compact( CLM_TYP_CD, CHNG_OVR_IND,CLAIM_STATE_CODE, CLAIM_STATUS_CODE,  CLAIM_STATUS_REASON_CODE),''), '') is NULL  
				THEN MD5( '99999' ) ELSE
				md5(cast(
    
    coalesce(cast(CLM_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CHNG_OVR_IND as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_STATUS_REASON_CODE as 
    varchar
), '')

 as 
    varchar
))
				END AS CLAIM_TYPE_STATUS_HKEY 
	    , CASE WHEN nullif(array_to_string(array_construct_compact( POLICY_TYPE_CODE,PLCY_STS_TYP_CD,PLCY_STS_RSN_TYP_CD,POLICY_ACTIVE_IND),''), '') is NULL  
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
			END 											 AS 						 POLICY_STANDING_HKEY 
		, CASE WHEN WRNT_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(WRNT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(WRNT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(WRNT_DATE::VARCHAR,'-','')::INTEGER END AS WRNT_DATE_KEY		 
		, WRNT_NO                                            AS                                            WRNT_NO 
		, POLICY_NUMBER                                      AS                                      POLICY_NUMBER 
		, PATIENT_ACCOUNT_NUMBER                             AS                             PATIENT_ACCOUNT_NUMBER 
		, UNITS_OF_SERVICE                                   AS                                   UNITS_OF_SERVICE 
		, PAID_UNITS                                         AS                                         PAID_UNITS 
		, BILLED_AMOUNT                                      AS                                      BILLED_AMOUNT 
		, NTWK_BILLED_AMT                                    AS                                    NTWK_BILLED_AMT 
		, FEE_SCHED_AMOUNT                                   AS                                   FEE_SCHED_AMOUNT 
		, CALC_AMOUNT                                        AS                                        CALC_AMOUNT 
		, APPROVED_AMOUNT                                    AS                                    APPROVED_AMOUNT 
		, PMT_AMT                                            AS                                            PMT_AMT 
		, (COALESCE(APPROVED_AMOUNT,0) + COALESCE(PMT_AMT,0)) AS LINE_REIMBURSED_AMOUNT  
		, BASE_AMOUNT                                        AS                                        BASE_AMOUNT 
		, OUTLIER                                            AS                                            OUTLIER 
		, RURAL                                              AS                                              RURAL 
		, HOLD_HARMLESS                                      AS                                      HOLD_HARMLESS 
		, MEDICARE_AMOUNT                                    AS                                    MEDICARE_AMOUNT 
		, MARKUP                                             AS                                             MARKUP 
		, CALC_TOTAL                                         AS                                         CALC_TOTAL 
		, SERVICE_UNITS                                      AS                                      SERVICE_UNITS
		, PROVIDER_PAY_TO_COST                               AS                               PROVIDER_PAY_TO_COST 
		, PROVIDER_OUTPATIENT_CCR                            AS                            PROVIDER_OUTPATIENT_CCR 
		, PROVIDER_DEV_CCR                                   AS                                   PROVIDER_DEV_CCR 
		, PROVIDER_PUB_DEV_CCR                               AS                               PROVIDER_PUB_DEV_CCR 
		, PROVIDER_WAGE_INDEX                                AS                                PROVIDER_WAGE_INDEX 
		, BATCH_NUMBER                                       AS                                       BATCH_NUMBER 
		, CLAIM_NUMBER                                       AS                                       CLAIM_NUMBER 
		, INVOICE_TYPE_DESC                                  AS                                  INVOICE_TYPE_DESC 
		, MCO_NUMBER                                         AS                                         MCO_NUMBER 
		, SERVICING_PEACH_NUMBER                             AS                             SERVICING_PEACH_NUMBER 
		, PAYTO_PEACH_NUMBER                                 AS                                 PAYTO_PEACH_NUMBER 
		, PROCEDURE_CODE                                     AS                                     PROCEDURE_CODE 
		, PLACE_OF_SERVICE_CODE                              AS                              PLACE_OF_SERVICE_CODE 
		, REFERRING_PEACH_NUMBER                             AS                             REFERRING_PEACH_NUMBER 
		, DIAGNOSIS_CODE                                     AS                                     DIAGNOSIS_CODE 
		, DIAGNOSIS1                                         AS                                         DIAGNOSIS1 
		, DIAGNOSIS2                                         AS                                         DIAGNOSIS2 
		, DIAGNOSIS3                                         AS                                         DIAGNOSIS3 
		, DIAGNOSIS4                                         AS                                         DIAGNOSIS4 
		, PAID_MCO_NUMBER                                    AS                                    PAID_MCO_NUMBER 
		, CUST_NO                                            AS                                            CUST_NO 
		, INVOICE_TYPE                                       AS                                       INVOICE_TYPE 
		, SUBROGATION_FLAG                                   AS                                   SUBROGATION_FLAG 
		, ADJUSTMENT_TYPE                                    AS                                    ADJUSTMENT_TYPE 
		, INPUT_METHOD_CODE                                  AS                                  INPUT_METHOD_CODE 
		, PAYMENT_CATEGORY                                   AS                                   PAYMENT_CATEGORY 
		, FEE_SCHEDULE                                       AS                                       FEE_SCHEDULE 
		, MOD1_MODIFIER_CODE                                 AS                                 MOD1_MODIFIER_CODE 
		, MOD2_MODIFIER_CODE                                 AS                                 MOD2_MODIFIER_CODE 
		, MOD3_MODIFIER_CODE                                 AS                                 MOD3_MODIFIER_CODE 
		, MOD4_MODIFIER_CODE                                 AS                                 MOD4_MODIFIER_CODE 
		, MOD_SET                                            AS                                            MOD_SET 
		, REVENUE_CENTER_CODE                                AS                                REVENUE_CENTER_CODE 
		, ADMISSION_TYPE                                     AS                                     ADMISSION_TYPE 
		, ADMISSION_SOURCE                                   AS                                   ADMISSION_SOURCE 
		, DISCHARGE_STATUS                                   AS                                   DISCHARGE_STATUS 
		, BILL_TYPE                                          AS                                          BILL_TYPE 
		, ADMISSION_DATE                                     AS                                     ADMISSION_DATE 
		, DISCHARGE_DATE                                     AS                                     DISCHARGE_DATE 
		, APC_CODE                                           AS                                           APC_CODE 
		, APC_STATUS                                         AS                                         APC_STATUS 
		, NON_OPPS_BILL_TYPE                                 AS                                 NON_OPPS_BILL_TYPE 
		, OPPS_FLAG                                          AS                                          OPPS_FLAG 
		, PAYMENT_METHOD                                     AS                                     PAYMENT_METHOD 
		, DISCOUNT                                           AS                                           DISCOUNT 
		, COMPOSITE_ADJUSTMENT                               AS                               COMPOSITE_ADJUSTMENT 
		, PAYMENT_INDICATOR                                  AS                                  PAYMENT_INDICATOR 
		, PACKAGING                                          AS                                          PACKAGING 
		, PAYMENT_ADJUSTMENT                                 AS                                 PAYMENT_ADJUSTMENT 
		, ADMITTING_DIAGNOSIS_CODE                           AS                           ADMITTING_DIAGNOSIS_CODE 
		, CLAIM_STATE_CODE                                   AS                                   CLAIM_STATE_CODE 
		, CLAIM_STATUS_CODE                                  AS                                  CLAIM_STATUS_CODE 
		, CLAIM_STATUS_REASON_CODE                           AS                           CLAIM_STATUS_REASON_CODE 
		, PAID_ABOVE_ZERO_IND                                AS                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              AS                              SUBROGATION_TYPE_DESC
		, CLM_TYP_CD                                         AS                                         CLM_TYP_CD 
		from SRC_INVOICE
            ),
LOGIC_RC as ( SELECT 
		  REVENUE_CENTER_HKEY                                AS                                REVENUE_CENTER_HKEY 
		, HOSPITAL_REVENUE_CENTER_CODE                       AS                       HOSPITAL_REVENUE_CENTER_CODE 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_RC
            ),
LOGIC_APC as ( SELECT 
		  APC_HKEY                                           AS                                           APC_HKEY 
		, APC_CODE                                           AS                                           APC_CODE 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_APC
            ),
LOGIC_NTWK as ( SELECT 
		  NETWORK_HKEY                                       AS                                       NETWORK_HKEY 
		, NETWORK_NUMBER                                     AS                                     NETWORK_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_NTWK
            ),
LOGIC_S_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      AS                                      PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              AS                              PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_S_PRVDR
            ),
LOGIC_P_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      AS                                      PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              AS                              PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_P_PRVDR
            ),
LOGIC_CPT as ( SELECT 
		  CPT_HKEY                                           AS                                           CPT_HKEY 
		, PROCEDURE_CODE                                     AS                                     PROCEDURE_CODE 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_CPT
            ),
LOGIC_A_ICD as ( SELECT 
		  ICD_HKEY                                           AS                                           ICD_HKEY 
		, ICD_CODE                                           AS                                           ICD_CODE 
		, ICD_CODE_VERSION_NUMBER                            AS                            ICD_CODE_VERSION_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_A_ICD
            ),
LOGIC_P_ICD as ( SELECT 
		  ICD_HKEY                                           AS                                           ICD_HKEY 
		, ICD_CODE                                           AS                                           ICD_CODE 
		, ICD_CODE_VERSION_NUMBER                            AS                            ICD_CODE_VERSION_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_P_ICD
            ),
LOGIC_P_NTWK as ( SELECT 
		  NETWORK_HKEY                                       AS                                       NETWORK_HKEY 
		, NETWORK_NUMBER                                     AS                                     NETWORK_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_P_NTWK
            ),
LOGIC_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                AS                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    AS                                    CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              AS                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    AS                                    RECORD_END_DATE 
		from SRC_IW
            )

---- RENAME LAYER ----
,

RENAME_INVOICE as ( SELECT 
		  INVOICE_NUMBER                                     as                             MEDICAL_INVOICE_NUMBER
		, LINE_SEQUENCE                                      as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, SEQUENCE_EXTENSION                                 as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, LINE_VERSION_NUMBER                                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, LINE_DLM_KEY                                       as                           SOURCE_LINE_DLM_DATE_KEY
		, HEADER_VERSION_NUMBER                              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, ADMISSION_HKEY                                     as                                     ADMISSION_HKEY
		, ADMISSION_DATE_KEY                                 as                                 ADMISSION_DATE_KEY
		, DISCHARGE_DATE_KEY                                 as                                 DISCHARGE_DATE_KEY
		, GROUPER_HKEY                                       as 									  GROUPER_HKEY
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY
		, LINE_STATUS_CODE_HKEY                              as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_STATUS_HKEY                                as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, ADJUDICATION_STATUS_DATE_KEY                       as                          BWC_ADJUDICATION_DATE_KEY
		, PAYMENT_DATE_KEY                                   as                                      PAID_DATE_KEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, POLICY_STANDING_HKEY								 as 							  POLICY_STANDING_HKEY
		, WRNT_DATE_KEY                                      as                          ORIGINAL_WARRANT_DATE_KEY
		, LINE_SERVICE_FROM_DATE                             AS                             LINE_SERVICE_FROM_DATE 
		, INVOICE_SERVICE_FROM_DATE                          AS                          INVOICE_SERVICE_FROM_DATE 
		, WRNT_NO                                            as                            ORIGINAL_WARRANT_NUMBER
		, POLICY_NUMBER                                      as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, PATIENT_ACCOUNT_NUMBER                             as                             PATIENT_ACCOUNT_NUMBER
		, UNITS_OF_SERVICE                                   as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, PAID_UNITS                                         as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, BILLED_AMOUNT                                      as                        LINE_PROVIDER_BILLED_AMOUNT
		, NTWK_BILLED_AMT                                    as                            LINE_MCO_ALLOWED_AMOUNT
		, FEE_SCHED_AMOUNT                                   as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, CALC_AMOUNT                                        as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, APPROVED_AMOUNT                                    as                           LINE_BWC_APPROVED_AMOUNT
		, PMT_AMT                                            as                               LINE_INTEREST_AMOUNT
        , LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, BASE_AMOUNT                                        as                            PRICER_OPPS_BASE_AMOUNT
		, OUTLIER                                            as                              PRICER_OUTLIER_AMOUNT
		, RURAL                                              as                         PRICER_RURAL_ADD_ON_AMOUNT
		, HOLD_HARMLESS                                      as                 PRICER_HOLD_HARMLESS_ADD_ON_AMOUNT
		, MEDICARE_AMOUNT                                    as                  PRICER_MEDICARE_REIMBURSED_AMOUNT
		, MARKUP                                             as                          PRICER_BWC_MARK_UP_AMOUNT
		, CALC_TOTAL                                         as                            PRICER_CALC_PAID_AMOUNT
		, SERVICE_UNITS                                      as                               GROUPER_SERVICE_UNIT
		, PROVIDER_PAY_TO_COST                               as                               PROVIDER_PAY_TO_COST
		, PROVIDER_OUTPATIENT_CCR                            as                            PROVIDER_OUTPATIENT_CCR
		, PROVIDER_DEV_CCR                                   as                                PROVIDER_DEVICE_CCR
		, PROVIDER_PUB_DEV_CCR                               as                            PROVIDER_PUB_DEVICE_CCR
		, PROVIDER_WAGE_INDEX                                as                                PROVIDER_WAGE_INDEX
		, BATCH_NUMBER                                       as                               INVOICE_BATCH_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
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
		, CUST_NO                                            as                            INVOICE_CUSTOMER_NUMBER
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
		, MOD_SET                                            as                               INVOICE_MODIFER_TYPE
		, RECEIPT_DATE                                       as                               INVOICE_RECEIPT_DATE
		, REVENUE_CENTER_CODE                                as                                REVENUE_CENTER_CODE
		, ADMISSION_TYPE                                     as                                     ADMISSION_TYPE
		, ADMISSION_SOURCE                                   as                                   ADMISSION_SOURCE
		, DISCHARGE_STATUS                                   as                                   DISCHARGE_STATUS
		, BILL_TYPE                                          as                                          BILL_TYPE
		, ADMISSION_DATE                                     as                                     ADMISSION_DATE
		, DISCHARGE_DATE                                     as                                     DISCHARGE_DATE
		, APC_CODE                                           as                                           APC_CODE
		, APC_STATUS                                         as                                         APC_STATUS
		, NON_OPPS_BILL_TYPE                                 as                                 NON_OPPS_BILL_TYPE
		, OPPS_FLAG                                          as                                          OPPS_FLAG
		, PAYMENT_METHOD                                     as                                     PAYMENT_METHOD
		, DISCOUNT                                           as                                           DISCOUNT
		, COMPOSITE_ADJUSTMENT                               as                               COMPOSITE_ADJUSTMENT
		, PAYMENT_INDICATOR                                  as                                  PAYMENT_INDICATOR
		, PACKAGING                                          as                                          PACKAGING
		, PAYMENT_ADJUSTMENT                                 as                                 PAYMENT_ADJUSTMENT
		, ADMITTING_DIAGNOSIS_CODE                           as                           ADMITTING_DIAGNOSIS_CODE
		, CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
				FROM     LOGIC_INVOICE   ),
RENAME_RC as ( SELECT 
		  REVENUE_CENTER_HKEY                                as                                REVENUE_CENTER_HKEY
		, HOSPITAL_REVENUE_CENTER_CODE                       as                       HOSPITAL_REVENUE_CENTER_CODE
		, RECORD_EFFECTIVE_DATE                              as                           RC_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                 RC_RECORD_END_DATE 
				FROM     LOGIC_RC   ), 
RENAME_APC as ( SELECT 
		  APC_HKEY                                           as                                           APC_HKEY
		, APC_CODE                                           as                                       APC_APC_CODE
		, RECORD_EFFECTIVE_DATE                              as                          APC_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                APC_RECORD_END_DATE 
				FROM     LOGIC_APC   ), 
RENAME_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                            SUBMITTING_NETWORK_HKEY
		, NETWORK_NUMBER                                     as                                NTWK_NETWORK_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                         NTWK_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                               NTWK_RECORD_END_DATE 
				FROM     LOGIC_NTWK   ), 
RENAME_S_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                              SERVICE_PROVIDER_HKEY
		, PROVIDER_PEACH_NUMBER                              as                      S_PRVDR_PROVIDER_PEACH_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                      S_PRVDR_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                            S_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_S_PRVDR   ), 
RENAME_P_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                               PAY_TO_PROVIDER_HKEY
		, PROVIDER_PEACH_NUMBER                              as                      P_PRVDR_PROVIDER_PEACH_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                      P_PRVDR_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                            P_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_P_PRVDR   ), 
RENAME_CPT as ( SELECT 
		  CPT_HKEY                                           as                          CPT_SERVICE_RENDERED_HKEY
		, PROCEDURE_CODE                                     as                                 CPT_PROCEDURE_CODE
		, RECORD_EFFECTIVE_DATE                              as                          CPT_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                CPT_RECORD_END_DATE 
				FROM     LOGIC_CPT   ), 
RENAME_A_ICD as ( SELECT 
		  ICD_HKEY                                           as                                 ADMITTING_ICD_HKEY
		, ICD_CODE                                           as                                     A_ICD_ICD_CODE
		, ICD_CODE_VERSION_NUMBER                            as                          A_ICD_CODE_VERSION_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                               A_ICD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                     A_ICD_END_DATE 
				FROM     LOGIC_A_ICD   ), 
RENAME_P_ICD as ( SELECT 
		  ICD_HKEY                                           as                                 PRINCIPAL_ICD_HKEY
		, ICD_CODE                                           as                                     P_ICD_ICD_CODE
		, ICD_CODE_VERSION_NUMBER                            as                          P_ICD_CODE_VERSION_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                               P_ICD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                     P_ICD_END_DATE 
				FROM     LOGIC_P_ICD   ), 
RENAME_P_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                PAY_TO_NETWORK_HKEY
		, NETWORK_NUMBER                                     as                              P_NTWK_NETWORK_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                       P_NTWK_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                             P_NTWK_RECORD_END_DATE 
				FROM     LOGIC_P_NTWK   ), 
RENAME_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
		, CUSTOMER_NUMBER                                    as                       IW_CORESUITE_CUSTOMER_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                           IW_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                 IW_RECORD_END_DATE 
				FROM     LOGIC_IW   )
/*--RENAME_CLM as ( SELECT 
		--  CLAIM_HKEY                                         as                                  CLAIM_NUMBER_HKEY
		--, CLAIM_NUMBER                                       as                                   CLM_CLAIM_NUMBER 
		--		FROM     LOGIC_CLM   ) , 
RENAME_R_PRVDR as ( SELECT 
		  PROVIDER_PEACH_NUMBER                              as                      R_PRVDR_PROVIDER_PEACH_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                      R_PRVDR_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                            R_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_R_PRVDR   ) */

---- FILTER LAYER (uses aliases) ----
,
FILTER_INVOICE                        as ( SELECT * from    RENAME_INVOICE 
				WHERE INVOICE_INVOICE_TYPE in ('HO')  ),
FILTER_NTWK                           as ( SELECT * from    RENAME_NTWK   ),
FILTER_S_PRVDR                        as ( SELECT * from    RENAME_S_PRVDR   ),
FILTER_P_PRVDR                        as ( SELECT * from    RENAME_P_PRVDR   ),
FILTER_CPT                            as ( SELECT * from    RENAME_CPT   ),
-- FILTER_R_PRVDR                        as ( SELECT * from    RENAME_R_PRVDR   ),
FILTER_A_ICD                          as ( SELECT * from    RENAME_A_ICD   ),
FILTER_P_ICD                          as ( SELECT * from    RENAME_P_ICD   ),
FILTER_P_NTWK                         as ( SELECT * from    RENAME_P_NTWK   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
--FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_RC                             as ( SELECT * from    RENAME_RC   ),
FILTER_APC                            as ( SELECT * from    RENAME_APC   ),

---- JOIN LAYER ----
INVOICE AS ( SELECT * 
			FROM  FILTER_INVOICE
				INNER JOIN FILTER_NTWK ON COALESCE(FILTER_INVOICE.INVOICE_MCO_NUMBER, '00000') = FILTER_NTWK.NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN NTWK_RECORD_EFFECTIVE_DATE AND COALESCE(NTWK_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_S_PRVDR ON COALESCE(FILTER_INVOICE.INVOICE_SERVICING_PEACH_NUMBER, '99999999999') = FILTER_S_PRVDR.S_PRVDR_PROVIDER_PEACH_NUMBER AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN S_PRVDR_RECORD_EFFECTIVE_DATE AND COALESCE( S_PRVDR_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_P_PRVDR ON COALESCE(FILTER_INVOICE.INVOICE_PAYTO_PEACH_NUMBER,'99999999999') = FILTER_P_PRVDR.P_PRVDR_PROVIDER_PEACH_NUMBER AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN P_PRVDR_RECORD_EFFECTIVE_DATE AND COALESCE( P_PRVDR_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_CPT ON COALESCE(FILTER_INVOICE.INVOICE_PROCEDURE_CODE, 'UNK') = FILTER_CPT.CPT_PROCEDURE_CODE AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN CPT_RECORD_EFFECTIVE_DATE AND COALESCE( CPT_RECORD_END_DATE, '2099-12-31') 
				-- LEFT JOIN FILTER_R_PRVDR ON COALESCE(FILTER_INVOICE.INVOICE_REFERRING_PEACH_NUMBER, '99999999999') = FILTER_R_PRVDR.R_PRVDR_PROVIDER_PEACH_NUMBER AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN R_PRVDR_RECORD_EFFECTIVE_DATE AND COALESCE( R_PRVDR_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_A_ICD ON COALESCE(FILTER_INVOICE.ADMITTING_DIAGNOSIS_CODE,  'UNK') = FILTER_A_ICD.A_ICD_ICD_CODE AND INVOICE_SERVICE_FROM_DATE BETWEEN A_ICD_EFFECTIVE_DATE AND COALESCE( A_ICD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_P_ICD ON COALESCE(FILTER_INVOICE.INVOICE_DIAGNOSIS_CODE,  'UNK') = FILTER_P_ICD.P_ICD_ICD_CODE AND INVOICE_SERVICE_FROM_DATE BETWEEN P_ICD_EFFECTIVE_DATE AND COALESCE( P_ICD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_P_NTWK ON COALESCE(FILTER_INVOICE.INVOICE_PAID_MCO, '00000') = FILTER_P_NTWK.P_NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN P_NTWK_RECORD_EFFECTIVE_DATE AND COALESCE(P_NTWK_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_IW ON COALESCE(FILTER_INVOICE.INVOICE_CUSTOMER_NUMBER, '99999') = FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN IW_RECORD_EFFECTIVE_DATE AND COALESCE( IW_RECORD_END_DATE, '2099-12-31') 
				--LEFT JOIN FILTER_CLM ON COALESCE(FILTER_INVOICE.CLAIM_NUMBER, 'UNK') = FILTER_CLM.CLM_CLAIM_NUMBER 
				LEFT JOIN FILTER_RC ON COALESCE(FILTER_INVOICE.REVENUE_CENTER_CODE, '9999') = FILTER_RC.HOSPITAL_REVENUE_CENTER_CODE AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN RC_RECORD_EFFECTIVE_DATE AND COALESCE( RC_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_APC ON COALESCE(FILTER_INVOICE.APC_CODE, 'UNK') = FILTER_APC.APC_APC_CODE AND  COALESCE(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN APC_RECORD_EFFECTIVE_DATE AND COALESCE( APC_RECORD_END_DATE, '2099-12-31')  )
,
ETL1 AS (SELECT 
-- DISTINCT    -- THE DISTINCT KEYWORD IS USED JUST BECAUSE CLAIM DIM HAS ISSUE. SEEING MORE THAN ONE ROW PER CLAIM. 
MEDICAL_INVOICE_NUMBER,
MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER,
MEDICAL_INVOICE_LINE_EXTENSION_NUMBER,
MEDICAL_INVOICE_LINE_VERSION_NUMBER,
SOURCE_LINE_DLM_DATE_KEY,
MEDICAL_INVOICE_HEADER_VERSION_NUMBER,
ADMISSION_HKEY,
ADMISSION_DATE_KEY,
DISCHARGE_DATE_KEY,
COALESCE(REVENUE_CENTER_HKEY, md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS REVENUE_CENTER_HKEY,
COALESCE(APC_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS APC_HKEY,
GROUPER_HKEY,
INVOICE_PROFILE_HKEY,
COALESCE(SUBMITTING_NETWORK_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS SUBMITTING_NETWORK_HKEY,
BWC_BILL_RECEIPT_DATE_KEY,
NETWORK_RECEIPT_DATE_KEY,
COALESCE(SERVICE_PROVIDER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS SERVICE_PROVIDER_HKEY,
COALESCE(PAY_TO_PROVIDER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PAY_TO_PROVIDER_HKEY,
COALESCE(CPT_SERVICE_RENDERED_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS CPT_SERVICE_RENDERED_HKEY,
MODIFIER_SEQUENCE_HKEY,
INVOICE_SERVICE_FROM_DATE_KEY,
INVOICE_SERVICE_TO_DATE_KEY,
LINE_SERVICE_FROM_DATE_KEY,
LINE_SERVICE_TO_DATE_KEY,
COALESCE(ADMITTING_ICD_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS ADMITTING_ICD_HKEY,
COALESCE(PRINCIPAL_ICD_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PRINCIPAL_ICD_HKEY,
INVOICE_LINE_ITEM_STATUS_HKEY,
INVOICE_HEADER_CURRENT_STATUS_HKEY,
COALESCE(PAY_TO_NETWORK_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PAY_TO_NETWORK_HKEY,
INTEREST_ACCRUAL_DATE_KEY,
BWC_ADJUDICATION_DATE_KEY,
PAID_DATE_KEY,
COALESCE(INJURED_WORKER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS INJURED_WORKER_HKEY,
CLAIM_DETAIL_HKEY,
COALESCE(CLAIM_TYPE_STATUS_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS CLAIM_TYPE_STATUS_HKEY,
POLICY_STANDING_HKEY,
ORIGINAL_WARRANT_DATE_KEY AS ORIGINAL_WARRANT_DATE_KEY,
ORIGINAL_WARRANT_NUMBER AS ORIGINAL_WARRANT_NUMBER,
INVOICE_SUBMITTED_POLICY_NUMBER,
PATIENT_ACCOUNT_NUMBER,
CLAIM_NUMBER,
LINE_UNITS_OF_BILLED_SERVICE_COUNT::NUMERIC(22,2) AS LINE_UNITS_OF_BILLED_SERVICE_COUNT,
LINE_UNITS_OF_PAID_SERVICE_COUNT::NUMERIC(22,2) AS LINE_UNITS_OF_PAID_SERVICE_COUNT,
LINE_PROVIDER_BILLED_AMOUNT::NUMERIC(32,2) AS LINE_PROVIDER_BILLED_AMOUNT,
LINE_MCO_ALLOWED_AMOUNT::NUMERIC(32,2) AS LINE_MCO_ALLOWED_AMOUNT,
LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT::NUMERIC(32,2) AS LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT,
LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT::NUMERIC(32,2) AS LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT,
LINE_BWC_APPROVED_AMOUNT::NUMERIC(32,2) AS LINE_BWC_APPROVED_AMOUNT,
LINE_INTEREST_AMOUNT::NUMERIC(32,2) AS LINE_INTEREST_AMOUNT,
LINE_REIMBURSED_AMOUNT::NUMERIC(32,2) AS LINE_REIMBURSED_AMOUNT,
PRICER_OPPS_BASE_AMOUNT::NUMERIC(32,2) AS PRICER_OPPS_BASE_AMOUNT,
PRICER_OUTLIER_AMOUNT::NUMERIC(32,2) AS PRICER_OUTLIER_AMOUNT,
PRICER_RURAL_ADD_ON_AMOUNT::NUMERIC(32,2) AS PRICER_RURAL_ADD_ON_AMOUNT,
PRICER_HOLD_HARMLESS_ADD_ON_AMOUNT::NUMERIC(32,2) AS PRICER_HOLD_HARMLESS_ADD_ON_AMOUNT,
PRICER_MEDICARE_REIMBURSED_AMOUNT::NUMERIC(32,2) AS PRICER_MEDICARE_REIMBURSED_AMOUNT,
PRICER_BWC_MARK_UP_AMOUNT::NUMERIC(32,2) AS PRICER_BWC_MARK_UP_AMOUNT,
PRICER_CALC_PAID_AMOUNT::NUMERIC(32,2) AS PRICER_CALC_PAID_AMOUNT,
GROUPER_SERVICE_UNIT::NUMERIC(32,2) AS GROUPER_SERVICE_UNIT,
PROVIDER_PAY_TO_COST::NUMERIC(8,5) AS PROVIDER_PAY_TO_COST,
PROVIDER_OUTPATIENT_CCR::NUMERIC(8,5) AS PROVIDER_OUTPATIENT_CCR,
PROVIDER_DEVICE_CCR::NUMERIC(8,5) AS PROVIDER_DEVICE_CCR,
PROVIDER_PUB_DEVICE_CCR::NUMERIC(8,5) AS PROVIDER_PUB_DEVICE_CCR,
PROVIDER_WAGE_INDEX::NUMERIC(8,5) AS PROVIDER_WAGE_INDEX,
CURRENT_TIMESTAMP() AS LOAD_DATETIME,
'CAM' AS PRIMARY_SOURCE_SYSTEM,
INVOICE_BATCH_NUMBER
 FROM INVOICE
)

SELECT * FROM ETL1
      );
    