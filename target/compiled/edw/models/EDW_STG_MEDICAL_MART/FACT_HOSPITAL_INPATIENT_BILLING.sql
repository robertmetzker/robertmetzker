

---- SRC LAYER ----
WITH
SRC_INVOICE as ( SELECT *     from     STAGING.DSV_INVOICE_HOSPITAL ),
SRC_DRG     as ( SELECT *     from     EDW_STAGING_DIM.DIM_DIAGNOSIS_RELATED_GROUP ),
SRC_RC      as ( SELECT *     from     EDW_STAGING_DIM.DIM_REVENUE_CENTER ),
SRC_S_NTWK  as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_S_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_P_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_CPT     as ( SELECT *     from     EDW_STAGING_DIM.DIM_CPT ),
SRC_R_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_A_ICD   as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_ICD   as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_NTWK  as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_IW      as ( SELECT *     from     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
SRC_CLM     as ( SELECT *     from     EDW_STAGING_DIM.DIM_CLAIM ),
SRC_BWC_DRG as ( SELECT *     from     EDW_STAGING_DIM.DIM_BWC_CALC_DRG_OUTPUT ),
//SRC_INVOICE as ( SELECT *     from     DSV_INVOICE_HOSPITAL) ,
//SRC_DRG     as ( SELECT *     from     DIM_DIAGNOSIS_RELATED_GROUP) ,
//SRC_RC      as ( SELECT *     from     DIM_REVENUE_CENTER) ,
//SRC_S_NTWK  as ( SELECT *     from     DIM_NETWORK) ,
//SRC_S_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_P_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_CPT     as ( SELECT *     from     DIM_CPT) ,
//SRC_R_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_A_ICD   as ( SELECT *     from     DIM_ICD) ,
//SRC_P_ICD   as ( SELECT *     from     DIM_ICD) ,
//SRC_P_NTWK  as ( SELECT *     from     DIM_NETWORK) ,
//SRC_IW      as ( SELECT *     from     DIM_INJURED_WORKER) ,
//SRC_CLM     as ( SELECT *     from     DIM_CLAIM) ,
//SRC_BWC_DRG as ( SELECT *     from     DIM_BWC_CALC_DRG_OUTPUT) ,

---- LOGIC LAYER ----

LOGIC_INVOICE as ( SELECT 
		  INVOICE_NUMBER                                     as                                     INVOICE_NUMBER 
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE 
		, SEQUENCE_EXTENSION                                 as                                 SEQUENCE_EXTENSION 
		, LINE_VERSION_NUMBER                                as                                LINE_VERSION_NUMBER 
		, CASE WHEN LINE_DLM IS NULL THEN -1
				WHEN REPLACE(CAST(LINE_DLM::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(LINE_DLM::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(CAST(LINE_DLM::DATE AS VARCHAR),'-','')::INTEGER 
				END                                          as                                       LINE_DLM_KEY 
		, HEADER_VERSION_NUMBER                              as                              HEADER_VERSION_NUMBER 
		,  CASE WHEN nullif(array_to_string(array_construct_compact( ADMISSION_TYPE, ADMISSION_SOURCE, DISCHARGE_STATUS, BILL_TYPE),''), '') is NULL  
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
				END				                             as                                     ADMISSION_HKEY 
		, ADMISSION_DATE                                     as                                     ADMISSION_DATE
		, CASE WHEN ADMISSION_DATE IS NULL THEN -1
				WHEN REPLACE(CAST(ADMISSION_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(ADMISSION_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(CAST(ADMISSION_DATE::DATE AS VARCHAR),'-','')::INTEGER 
				END							                 as                                 ADMISSION_DATE_KEY
		, CASE WHEN DISCHARGE_DATE IS NULL THEN -1
				WHEN REPLACE(CAST(DISCHARGE_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(DISCHARGE_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(CAST(DISCHARGE_DATE::DATE AS VARCHAR),'-','')::INTEGER 
				END	                         				 as                                 DISCHARGE_DATE_KEY 
		, DISCHARGE_DATE                                     as                                     DISCHARGE_DATE
		, CASE WHEN nullif(array_to_string(array_construct_compact(INVOICE_TYPE,SUBROGATION_FLAG,ADJUSTMENT_TYPE,INPUT_METHOD_CODE,PAYMENT_CATEGORY,FEE_SCHEDULE,PAID_ABOVE_ZERO_IND,SUBROGATION_TYPE_DESC),''), '') is NULL  
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
			END                                              as                               INVOICE_PROFILE_HKEY  
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
		, CASE WHEN RECEIPT_DATE IS NULL THEN -1	
				WHEN REPLACE(CAST(RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(RECEIPT_DATE::VARCHAR,'-','')::INTEGER 
				END 										 as 						 BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE                               as                               NETWORK_RECEIPT_DATE 
		, CASE WHEN NETWORK_RECEIPT_DATE IS NULL THEN -1
			WHEN REPLACE(CAST(NETWORK_RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
			WHEN REPLACE(CAST(NETWORK_RECEIPT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3	
			ELSE REPLACE(NETWORK_RECEIPT_DATE::VARCHAR,'-','')::INTEGER END 	
															 as                 		  NETWORK_RECEIPT_DATE_KEY	
		, CASE WHEN nullif(array_to_string(array_construct_compact(MOD1_MODIFIER_CODE,MOD2_MODIFIER_CODE,MOD3_MODIFIER_CODE,MOD4_MODIFIER_CODE,MOD_SET),''), '') is NULL  
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
			END 											 as 							MODIFIER_SEQUENCE_HKEY 
		, CASE WHEN INVOICE_SERVICE_FROM_DATE IS NULL THEN -1
			WHEN REPLACE(CAST(INVOICE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
			WHEN REPLACE(CAST(INVOICE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
			ELSE REPLACE(INVOICE_SERVICE_FROM_DATE::VARCHAR,'-','')::INTEGER 
			END												 as                      INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_FROM_DATE                          AS                          INVOICE_SERVICE_FROM_DATE 
		, CASE WHEN INVOICE_SERVICE_TO_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(INVOICE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(INVOICE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(INVOICE_SERVICE_TO_DATE::VARCHAR,'-','')::INTEGER 
				END 										 as 					   INVOICE_SERVICE_TO_DATE_KEY 
		, INVOICE_SERVICE_TO_DATE                            AS                            INVOICE_SERVICE_TO_DATE 
		, CASE WHEN  LINE_SERVICE_FROM_DATE IS NULL THEN -1
				WHEN REPLACE(CAST( LINE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST( LINE_SERVICE_FROM_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE( LINE_SERVICE_FROM_DATE::VARCHAR,'-','')::INTEGER 
				END											 AS						    LINE_SERVICE_FROM_DATE_KEY 
		, LINE_SERVICE_FROM_DATE                             AS                             LINE_SERVICE_FROM_DATE
		, CASE WHEN  LINE_SERVICE_TO_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST( LINE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST( LINE_SERVICE_TO_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE( LINE_SERVICE_TO_DATE::VARCHAR,'-','')::INTEGER 
				END AS LINE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_TO_DATE                               as                               LINE_SERVICE_TO_DATE 
		, LINE_STATUS_CODE                                   as                                   LINE_STATUS_CODE 
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
)) END 
															 as                              LINE_STATUS_CODE_HKEY 
		, INVOICE_STATUS                                     as                                     INVOICE_STATUS 
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
)) END 
				                                             as                                INVOICE_STATUS_HKEY
		, INTEREST_ACCRUAL_DATE                              as                              INTEREST_ACCRUAL_DATE 
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
		, CASE WHEN nullif(array_to_string(array_construct_compact(CLM_TYP_CD, CHNG_OVR_IND, CLAIM_STATE_CODE, CLAIM_STATUS_CODE,  CLAIM_STATUS_REASON_CODE),''), '') is NULL  
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
				END AS ADJUDICATION_CLAIM_TYPE_STATUS_HKEY  
		, CASE WHEN nullif(array_to_string(array_construct_compact(POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND),''), '') is NULL  
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
				END AS  POLICY_STANDING_HKEY
		, CASE WHEN WRNT_DATE IS NULL THEN -1 
				WHEN REPLACE(CAST(WRNT_DATE::DATE AS VARCHAR),'-','')::INTEGER  < 19010101 THEN -2
				WHEN REPLACE(CAST(WRNT_DATE::DATE AS VARCHAR),'-','')::INTEGER  > 20991231 THEN -3
				ELSE REPLACE(WRNT_DATE::VARCHAR,'-','')::INTEGER END AS WRNT_DATE_KEY
		, WRNT_NO                                            as                                            WRNT_NO 
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER 
		, PATIENT_ACCOUNT_NUMBER                             as                             PATIENT_ACCOUNT_NUMBER 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, CASE WHEN DATEDIFF(DAYS, ADMISSION_DATE, DISCHARGE_DATE) <0 THEN -1 
				ELSE DATEDIFF(DAYS, ADMISSION_DATE, DISCHARGE_DATE) END           as HOSPITAL_LENGTH_OF_STAY_COUNT
        , UNITS_OF_SERVICE                                   as                                   UNITS_OF_SERVICE
		, PAID_UNITS                                         as                                         PAID_UNITS 
		, BILLED_AMOUNT                                      as                                      BILLED_AMOUNT 
		, NTWK_BILLED_AMT                                    as                                    NTWK_BILLED_AMT 
		, FEE_SCHED_AMOUNT                                   as                                   FEE_SCHED_AMOUNT 
		, CALC_AMOUNT                                        as                                        CALC_AMOUNT 
		, APPROVED_AMOUNT                                    as                                    APPROVED_AMOUNT 
		, PMT_AMT                                            as                                            PMT_AMT 
		,(NVL(APPROVED_AMOUNT,0) + NVL(PMT_AMT,0))           as                             LINE_REIMBURSED_AMOUNT 
		, IN_CCR                                             as                                             IN_CCR 
		, DGME                                               as                                               DGME 
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER 
		, INVOICE_TYPE_DESC                                  as                                  INVOICE_TYPE_DESC 
		, MCO_NUMBER                                         as                                         MCO_NUMBER 
		, SERVICING_PEACH_NUMBER                             as                             SERVICING_PEACH_NUMBER 
		, PAYTO_PEACH_NUMBER                                 as                                 PAYTO_PEACH_NUMBER 
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE 
		, PLACE_OF_SERVICE_CODE                              as                              PLACE_OF_SERVICE_CODE 
		, REFERRING_PEACH_NUMBER                             as                             REFERRING_PEACH_NUMBER 
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE 
		, DIAGNOSIS1                                         as                                         DIAGNOSIS1 
		, DIAGNOSIS2                                         as                                         DIAGNOSIS2 
		, DIAGNOSIS3                                         as                                         DIAGNOSIS3 
		, DIAGNOSIS4                                         as                                         DIAGNOSIS4 
		, PAID_MCO_NUMBER                                    as                                    PAID_MCO_NUMBER 
		, CUST_NO                                            as                                            CUST_NO 
		, INVOICE_TYPE                                       as                                       INVOICE_TYPE 
		, SUBROGATION_FLAG                                   as                                   SUBROGATION_FLAG 
		, ADJUSTMENT_TYPE                                    as                                    ADJUSTMENT_TYPE 
		, INPUT_METHOD_CODE                                  as                                  INPUT_METHOD_CODE 
		, PAYMENT_CATEGORY                                   as                                   PAYMENT_CATEGORY 
		, FEE_SCHEDULE                                       as                                       FEE_SCHEDULE 
		, MOD1_MODIFIER_CODE                                 as                                 MOD1_MODIFIER_CODE 
		, MOD2_MODIFIER_CODE                                 as                                 MOD2_MODIFIER_CODE 
		, MOD3_MODIFIER_CODE                                 as                                 MOD3_MODIFIER_CODE 
		, MOD4_MODIFIER_CODE                                 as                                 MOD4_MODIFIER_CODE 
		, MOD_SET                                            as                                            MOD_SET 
		, REVENUE_CENTER_CODE                                as                                REVENUE_CENTER_CODE 
		, FRMTTD_PRO_DRG                                     as                                     FRMTTD_PRO_DRG 
		, CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE 
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE 
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE 
		, ADMISSION_TYPE                                     as                                     ADMISSION_TYPE 
		, ADMISSION_SOURCE                                   as                                   ADMISSION_SOURCE 
		, DISCHARGE_STATUS                                   as                                   DISCHARGE_STATUS 
		, BILL_TYPE                                          as                                          BILL_TYPE 
		, ADMITTING_DIAGNOSIS_CODE                           as                           ADMITTING_DIAGNOSIS_CODE 
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND 
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
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
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
		, CASE WHEN nullif(array_to_string(array_construct_compact(OCCR_SRC_TYP_NM, OCCR_MEDA_TYP_NM, NOI_CTG_TYP_NM, NOI_TYP_NM,FIREFIGHTER_CANCER_IND,COVID_EXPOSURE_IND,COVID_EMERGENCY_WORKER_IND,COVID_HEALTH_CARE_WORKER_IND,COMBINED_CLAIM_IND,SB223_IND,EMPLOYER_PREMISES_IND,CLM_CTRPH_INJR_IND,K_PROGRAM_ENROLLMENT_DESC,K_PROGRAM_TYPE_DESC,K_PROGRAM_REASON_DESC),''), '') is NULL  
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
			   END AS CLAIM_DETAIL_HKEY 
		from SRC_INVOICE
            ),
LOGIC_RC as ( SELECT 
		  REVENUE_CENTER_HKEY                                as                                REVENUE_CENTER_HKEY 
		, HOSPITAL_REVENUE_CENTER_CODE                       as                       HOSPITAL_REVENUE_CENTER_CODE 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_RC
            ),
LOGIC_S_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                       NETWORK_HKEY 
		, NETWORK_NUMBER                                     as                                     NETWORK_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_S_NTWK
            ),
LOGIC_S_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                                      PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_S_PRVDR
            ),
LOGIC_P_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                                      PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_P_PRVDR
            ),
LOGIC_CPT as ( SELECT 
		  CPT_HKEY                                           as                                           CPT_HKEY 
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_CPT
            ),
LOGIC_R_PRVDR as ( SELECT 
          CASE WHEN PROVIDER_HKEY IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
				ELSE md5(cast(
    
    coalesce(cast(PROVIDER_HKEY as 
    varchar
), '')

 as 
    varchar
)) END
		                                                     as                                      PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as                              PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_R_PRVDR
            ),
LOGIC_A_ICD as ( SELECT 
		  ICD_HKEY                                           as                                           ICD_HKEY 
		, ICD_CODE                                           as                                           ICD_CODE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		from SRC_A_ICD
            ),
LOGIC_P_ICD as ( SELECT 
		  ICD_HKEY                                           as                                           ICD_HKEY 
		, ICD_CODE                                           as                                           ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, ICD_CODE_VERSION_NUMBER                            as                            ICD_CODE_VERSION_NUMBER 
		from SRC_P_ICD
            ),
LOGIC_P_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                       NETWORK_HKEY 
		, NETWORK_NUMBER                                     as                                     NETWORK_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_P_NTWK
            ),
LOGIC_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		from SRC_IW
            ),
LOGIC_DRG as ( SELECT 
		  DRG_HKEY                                           as                                           DRG_HKEY
		, DIAGNOSIS_RELATED_GROUP_CODE                       AS                       DIAGNOSIS_RELATED_GROUP_CODE  
		, DRG_EFFECTIVE_DATE                                 AS                                 DRG_EFFECTIVE_DATE 
		, DRG_EXPIRATION_DATE                                AS                                DRG_EXPIRATION_DATE 
		from SRC_DRG
            ),
LOGIC_BWC_DRG as ( SELECT 
		  BWC_CALC_DRG_OUTPUT_HKEY                           as                           BWC_CALC_DRG_OUTPUT_HKEY 
		, MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER 
		FROM SRC_BWC_DRG
            )			 --,
/*LOGIC_CLM as ( SELECT 

		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		from SRC_CLM
            )
*/
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
		, ADJUDICATION_CLAIM_TYPE_STATUS_HKEY                as                             CLAIM_TYPE_STATUS_HKEY
		, POLICY_STANDING_HKEY								 as 							  POLICY_STANDING_HKEY
		, WRNT_DATE_KEY                                      as                          ORIGINAL_WARRANT_DATE_KEY
		, WRNT_NO                                            as                            ORIGINAL_WARRANT_NUMBER
		, POLICY_NUMBER                                      as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, PATIENT_ACCOUNT_NUMBER                             as                             PATIENT_ACCOUNT_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, HOSPITAL_LENGTH_OF_STAY_COUNT                      as                      HOSPITAL_LENGTH_OF_STAY_COUNT
		, UNITS_OF_SERVICE                                   as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, PAID_UNITS                                         as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, BILLED_AMOUNT                                      as                        LINE_PROVIDER_BILLED_AMOUNT
		, NTWK_BILLED_AMT                                    as                            LINE_MCO_ALLOWED_AMOUNT
		, FEE_SCHED_AMOUNT                                   as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, CALC_AMOUNT                                        as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, APPROVED_AMOUNT                                    as                           LINE_BWC_APPROVED_AMOUNT
		, PMT_AMT                                            as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, IN_CCR                                             as                             PROVIDER_INPATIENT_CCR
		, DGME                                               as                            PROVIDER_INPATIENT_DGME
		, BATCH_NUMBER                                       as                               INVOICE_BATCH_NUMBER
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
		, ADMISSION_DATE                                     as                                     ADMISSION_DATE
		, DISCHARGE_DATE                                     as                                     DISCHARGE_DATE
		, REVENUE_CENTER_CODE                                as                                REVENUE_CENTER_CODE
		, FRMTTD_PRO_DRG                                     as                                     FRMTTD_PRO_DRG
		, INVOICE_SERVICE_FROM_DATE                          as                          INVOICE_SERVICE_FROM_DATE
		, CLAIM_STATE_CODE                                   as                                   CLAIM_STATE_CODE
		, CLAIM_STATUS_CODE                                  as                                  CLAIM_STATUS_CODE
		, CLAIM_STATUS_REASON_CODE                           as                           CLAIM_STATUS_REASON_CODE
		, ADMISSION_TYPE                                     as                                     ADMISSION_TYPE
		, ADMISSION_SOURCE                                   as                                   ADMISSION_SOURCE
		, DISCHARGE_STATUS                                   as                                   DISCHARGE_STATUS
		, BILL_TYPE                                          as                                          BILL_TYPE
		, LINE_SERVICE_FROM_DATE                             as                             LINE_SERVICE_FROM_DATE
		, LINE_SERVICE_TO_DATE                               as                               LINE_SERVICE_TO_DATE
		, ADMITTING_DIAGNOSIS_CODE                           as                           ADMITTING_DIAGNOSIS_CODE
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
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
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
        , CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
				FROM     LOGIC_INVOICE   ), 
RENAME_RC as ( SELECT 
		  REVENUE_CENTER_HKEY                                as                                REVENUE_CENTER_HKEY
		, HOSPITAL_REVENUE_CENTER_CODE                       as                       HOSPITAL_REVENUE_CENTER_CODE
		, RECORD_EFFECTIVE_DATE                              as                           RC_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                 RC_RECORD_END_DATE 
				FROM     LOGIC_RC   ), 
RENAME_S_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                            SUBMITTING_NETWORK_HKEY
		, NETWORK_NUMBER                                     as                              S_NTWK_NETWORK_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                       S_NTWK_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                             S_NTWK_RECORD_END_DATE 
				FROM     LOGIC_S_NTWK   ), 
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
RENAME_R_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                            REFERRING_PROVIDER_HKEY
		, PROVIDER_PEACH_NUMBER                              as                      R_PRVDR_PROVIDER_PEACH_NUMBER
		, RECORD_EFFECTIVE_DATE                              as                      R_PRVDR_RECORD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                            R_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_R_PRVDR   ), 
RENAME_A_ICD as ( SELECT 
		  ICD_HKEY                                           as                                 ADMITTING_ICD_HKEY
		, ICD_CODE                                           as                                     A_ICD_ICD_CODE
		, RECORD_END_DATE                                    as                                     A_ICD_END_DATE
		, RECORD_EFFECTIVE_DATE                              as                               A_ICD_EFFECTIVE_DATE 
				FROM     LOGIC_A_ICD   ), 
RENAME_P_ICD as ( SELECT 
		  ICD_HKEY                                           as                                 PRINCIPAL_ICD_HKEY
		, ICD_CODE                                           as                                     P_ICD_ICD_CODE
		, RECORD_EFFECTIVE_DATE                              as                               P_ICD_EFFECTIVE_DATE
		, RECORD_END_DATE                                    as                                     P_ICD_END_DATE
		, ICD_CODE_VERSION_NUMBER                            as                          P_ICD_CODE_VERSION_NUMBER 
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
				FROM     LOGIC_IW   ), 
RENAME_DRG as ( SELECT 
		  DRG_HKEY                                           as                        PROVIDER_SUBMITTED_DRG_HKEY
		, DIAGNOSIS_RELATED_GROUP_CODE                       as                       DIAGNOSIS_RELATED_GROUP_CODE
		, DRG_EFFECTIVE_DATE                                 as                                 DRG_EFFECTIVE_DATE
		, DRG_EXPIRATION_DATE                                as                                DRG_EXPIRATION_DATE 
				FROM     LOGIC_DRG   ),
RENAME_BWC_DRG    as ( SELECT 
		  BWC_CALC_DRG_OUTPUT_HKEY                           as                           BWC_CALC_DRG_OUTPUT_HKEY
		, MEDICAL_INVOICE_NUMBER                             as                     BWC_DRG_MEDICAL_INVOICE_NUMBER 
				FROM     LOGIC_BWC_DRG   )				
				--, 
/*RENAME_CLM as ( SELECT 
		  CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, CLAIM_NUMBER                                       as                                   CLM_CLAIM_NUMBER 
				FROM     LOGIC_CLM   )
*/
---- FILTER LAYER (uses aliases) ----
,
FILTER_INVOICE                        as ( SELECT * from    RENAME_INVOICE 
                                            WHERE INVOICE_INVOICE_TYPE in ('HI')  ),
FILTER_S_NTWK                         as ( SELECT * from    RENAME_S_NTWK   ),
FILTER_P_ICD                          as ( SELECT * from    RENAME_P_ICD   ),
FILTER_A_ICD                          as ( SELECT * from    RENAME_A_ICD   ),
FILTER_P_NTWK                         as ( SELECT * from    RENAME_P_NTWK   ),
--FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_CPT                            as ( SELECT * from    RENAME_CPT   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
FILTER_S_PRVDR                        as ( SELECT * from    RENAME_S_PRVDR   ),
FILTER_P_PRVDR                        as ( SELECT * from    RENAME_P_PRVDR   ),
FILTER_R_PRVDR                        as ( SELECT * from    RENAME_R_PRVDR   ),
FILTER_RC                             as ( SELECT * from    RENAME_RC   ),
FILTER_DRG                            as ( SELECT * from    RENAME_DRG   ),
FILTER_BWC_DRG                        as ( SELECT * FROM    RENAME_BWC_DRG   ),

---- JOIN LAYER ----

INVOICE as ( SELECT * 
				FROM  FILTER_INVOICE
				LEFT JOIN FILTER_S_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_SERVICING_PEACH_NUMBER, '99999999999') =  FILTER_S_PRVDR.S_PRVDR_PROVIDER_PEACH_NUMBER AND coalesce(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN S_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( S_PRVDR_RECORD_END_DATE, '2099-12-31') 
						LEFT JOIN FILTER_S_NTWK ON  coalesce( FILTER_INVOICE.INVOICE_MCO_NUMBER, '00000') =  FILTER_S_NTWK.S_NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN S_NTWK_RECORD_EFFECTIVE_DATE AND coalesce( S_NTWK_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_ICD ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS_CODE,  'UNK') =  FILTER_P_ICD.P_ICD_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN P_ICD_EFFECTIVE_DATE AND coalesce( P_ICD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_A_ICD ON  coalesce( FILTER_INVOICE.ADMITTING_DIAGNOSIS_CODE,  'UNK') =  FILTER_A_ICD.A_ICD_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN A_ICD_EFFECTIVE_DATE AND coalesce( A_ICD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_NTWK ON  coalesce( FILTER_INVOICE.INVOICE_PAID_MCO, '00000') =  FILTER_P_NTWK.P_NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN P_NTWK_RECORD_EFFECTIVE_DATE AND coalesce( P_NTWK_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_CPT ON  coalesce( FILTER_INVOICE.INVOICE_PROCEDURE_CODE, 'UNK') =  FILTER_CPT.CPT_PROCEDURE_CODE AND coalesce(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN CPT_RECORD_EFFECTIVE_DATE AND coalesce( CPT_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_IW ON  coalesce( FILTER_INVOICE.INVOICE_CUSTOMER_NUMBER, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND INVOICE_SERVICE_FROM_DATE BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_PAYTO_PEACH_NUMBER, '99999999999') =  FILTER_P_PRVDR.P_PRVDR_PROVIDER_PEACH_NUMBER AND coalesce(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN P_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( P_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_R_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_REFERRING_PEACH_NUMBER, '99999999999') =  FILTER_R_PRVDR.R_PRVDR_PROVIDER_PEACH_NUMBER AND coalesce(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN R_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( R_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_RC ON  coalesce( FILTER_INVOICE.REVENUE_CENTER_CODE, '99999') =  FILTER_RC.HOSPITAL_REVENUE_CENTER_CODE AND coalesce(LINE_SERVICE_FROM_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN RC_RECORD_EFFECTIVE_DATE AND coalesce( RC_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_DRG ON  coalesce( FILTER_INVOICE.FRMTTD_PRO_DRG, 'UNK') =  FILTER_DRG.DIAGNOSIS_RELATED_GROUP_CODE AND coalesce(DISCHARGE_DATE,INVOICE_SERVICE_FROM_DATE) BETWEEN FILTER_DRG.DRG_EFFECTIVE_DATE AND coalesce(FILTER_DRG.DRG_EXPIRATION_DATE, '2099-12-31')
								LEFT JOIN FILTER_BWC_DRG ON  coalesce( FILTER_INVOICE.MEDICAL_INVOICE_NUMBER, 'UNK') =  FILTER_BWC_DRG.BWC_DRG_MEDICAL_INVOICE_NUMBER  )
		    
SELECT 
		  MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, ADMISSION_HKEY
		, ADMISSION_DATE_KEY
		, DISCHARGE_DATE_KEY
		, COALESCE(PROVIDER_SUBMITTED_DRG_HKEY, md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PROVIDER_SUBMITTED_DRG_HKEY
		, COALESCE(BWC_CALC_DRG_OUTPUT_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS BWC_CALC_DRG_OUTPUT_HKEY 
		, COALESCE(REVENUE_CENTER_HKEY, md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS REVENUE_CENTER_HKEY
		, INVOICE_PROFILE_HKEY
		, COALESCE(SUBMITTING_NETWORK_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS SUBMITTING_NETWORK_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY
		, COALESCE(SERVICE_PROVIDER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS SERVICE_PROVIDER_HKEY
		, COALESCE(PAY_TO_PROVIDER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PAY_TO_PROVIDER_HKEY
		, COALESCE(CPT_SERVICE_RENDERED_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS  CPT_SERVICE_RENDERED_HKEY
		, MODIFIER_SEQUENCE_HKEY
		, INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY
		, COALESCE(REFERRING_PROVIDER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS REFERRING_PROVIDER_HKEY
		, COALESCE(ADMITTING_ICD_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS ADMITTING_ICD_HKEY
		, COALESCE(PRINCIPAL_ICD_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PRINCIPAL_ICD_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY
		, COALESCE(PAY_TO_NETWORK_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS PAY_TO_NETWORK_HKEY
		, INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY
		, COALESCE(INJURED_WORKER_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS INJURED_WORKER_HKEY
		, COALESCE(CLAIM_DETAIL_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS CLAIM_DETAIL_HKEY
		, COALESCE(CLAIM_TYPE_STATUS_HKEY,md5(cast(
    
    coalesce(cast(-1111 as 
    varchar
), '')

 as 
    varchar
))) AS CLAIM_TYPE_STATUS_HKEY
		, POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER
		, PATIENT_ACCOUNT_NUMBER
		, CLAIM_NUMBER
		, HOSPITAL_LENGTH_OF_STAY_COUNT::NUMERIC(22,2) AS HOSPITAL_LENGTH_OF_STAY_COUNT
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT::NUMERIC(22,2) AS LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT::NUMERIC(22,2) AS LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT::NUMERIC(32,2) AS LINE_PROVIDER_BILLED_AMOUNT
		, LINE_MCO_ALLOWED_AMOUNT::NUMERIC(32,2) AS LINE_MCO_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT::NUMERIC(32,2) AS LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT::NUMERIC(32,2) AS LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT::NUMERIC(32,2) AS LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT::NUMERIC(32,2) AS LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT::NUMERIC(32,2) AS LINE_REIMBURSED_AMOUNT
		, PROVIDER_INPATIENT_CCR
		, PROVIDER_INPATIENT_DGME
		, CURRENT_TIMESTAMP() AS LOAD_DATETIME
		, 'CAM' AS PRIMARY_SOURCE_SYSTEM
		, INVOICE_BATCH_NUMBER 
from INVOICE