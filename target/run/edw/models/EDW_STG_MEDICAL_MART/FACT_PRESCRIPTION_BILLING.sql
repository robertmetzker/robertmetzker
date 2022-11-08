

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING  as
      (

---- SRC LAYER ----
WITH
SRC_INVOICE as ( SELECT *     from     STAGING.DSV_INVOICE_PRESCRIPTION ),
SRC_PRESCRIB_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_NDC as ( SELECT *     from     EDW_STAGING_DIM.DIM_NDC ),
SRC_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_S_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_P_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
SRC_R_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD1 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD2 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD3 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_ICD4 as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_P_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_IW as ( SELECT *     from     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
SRC_CLM as ( SELECT *     from     EDW_STAGING_DIM.DIM_CLAIM ),
SRC_R_PRVDR as ( SELECT *     from     EDW_STAGING_DIM.DIM_PROVIDER ),
//SRC_INVOICE as ( SELECT *     from     DSV_INVOICE_PRESCRIPTION) ,
//SRC_PRESCRIB_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_NDC as ( SELECT *     from     DIM_NDC) ,
//SRC_NTWK as ( SELECT *     from     DIM_NETWORK) ,
//SRC_S_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_P_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,
//SRC_R_ICD as ( SELECT *     from     DIM_ICD) ,
//SRC_P_ICD as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD1 as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD2 as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD3 as ( SELECT *     from     DIM_ICD) ,
//SRC_ICD4 as ( SELECT *     from     DIM_ICD) ,
//SRC_P_NTWK as ( SELECT *     from     DIM_NETWORK) ,
//SRC_IW as ( SELECT *     from     DIM_INJURED_WORKER) ,
//SRC_CLM as ( SELECT *     from     DIM_CLAIM) ,
//SRC_R_PRVDR as ( SELECT *     from     DIM_PROVIDER) ,

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
				END::INTEGER                                        
                                                             as                      	SOURCE_LINE_DLM_DATE_KEY 
		, HEADER_VERSION_NUMBER                              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER 
		, CASE WHEN nullif(array_to_string(array_construct_compact( SERVICE_LEVEL_CODE, PHARMACIST_SERVICE_CODE, SERVICE_REASON_CODE, SERVICE_RESULT_CODE, SUBMITTED_DAW_CODE, SUBMISSION_CLARIFICATION_CODE
							, PBM_BENEFIT_PLAN_TYPE_DESC, PBM_ORIGINATION_TYPE_DESC, PHARM_SPECIAL_PROGRAM_DESC, PBM_LOCK_IN_IND, PRESCRIPTION_REFILL_NUMBER ),''), '') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(SERVICE_LEVEL_CODE as 
    varchar
), '') || '-' || coalesce(cast(PHARMACIST_SERVICE_CODE as 
    varchar
), '') || '-' || coalesce(cast(SERVICE_REASON_CODE as 
    varchar
), '') || '-' || coalesce(cast(SERVICE_RESULT_CODE as 
    varchar
), '') || '-' || coalesce(cast(SUBMITTED_DAW_CODE as 
    varchar
), '') || '-' || coalesce(cast(SUBMISSION_CLARIFICATION_CODE as 
    varchar
), '') || '-' || coalesce(cast(PBM_BENEFIT_PLAN_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(PBM_ORIGINATION_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(PHARM_SPECIAL_PROGRAM_DESC as 
    varchar
), '') || '-' || coalesce(cast(PBM_LOCK_IN_IND as 
    varchar
), '') || '-' || coalesce(cast(PRESCRIPTION_REFILL_NUMBER as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                             PRESCRIPTION_BILL_HKEY 
		, CASE WHEN PRESCRIBED_DATE is null then '-1' 
			WHEN PRESCRIBED_DATE < '1901-01-01' then '-2' 
			WHEN PRESCRIBED_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PRESCRIBED_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                      PRESCRIPTION_WRITTEN_DATE_KEY 
		, CASE WHEN PBM_ADJUDICATION_DATE is null then '-1' 
			WHEN PBM_ADJUDICATION_DATE < '1901-01-01' then '-2' 
			WHEN PBM_ADJUDICATION_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PBM_ADJUDICATION_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                          PBM_ADJUDICATION_DATE_KEY 
		, CASE WHEN REBATE_DATE is null then '-1' 
			WHEN REBATE_DATE < '1901-01-01' then '-2' 
			WHEN REBATE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( REBATE_DATE, '[^0-9]+', '') 
				END::INTEGER                                       
                                                             as                      DRUG_REBATE_RECEIVED_DATE_KEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact( PRICE_TYPE, CLIENT_PRICING),''), '') is NULL  
			then MD5( '99999' ) 
			when  PRICE_TYPE in('MAC', 'S') then MD5('-1111') 
			ELSE md5(cast(
    
    coalesce(cast(PRICE_TYPE as 
    varchar
), '') || '-' || coalesce(cast(CLIENT_PRICING as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                            PBM_PRICING_METHOD_HKEY 
		,  CASE WHEN nullif(array_to_string(array_construct_compact( INVOICE_TYPE, SUBROGATION_FLAG, ADJUSTMENT_TYPE, INPUT_METHOD_CODE, PAYMENT_CATEGORY, FEE_SCHEDULE, PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC ),''), '') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
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
				END                                         
                                                             as                               INVOICE_PROFILE_HKEY  
		, CASE WHEN RECEIPT_DATE is null then '-1' 
			WHEN RECEIPT_DATE < '1901-01-01' then '-2' 
			WHEN RECEIPT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( RECEIPT_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                          BWC_BILL_RECEIPT_DATE_KEY 
		, CASE WHEN NETWORK_RECEIPT_DATE is null then '-1' 
			WHEN NETWORK_RECEIPT_DATE < '1901-01-01' then '-2' 
			WHEN NETWORK_RECEIPT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( NETWORK_RECEIPT_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                           NETWORK_RECEIPT_DATE_KEY 
		, MD5( '99999' )                                                as                  PHARMACY_STANDING_HKEY 
		, MD5( '99999' )                                                as      PRESCRIBING_PROVIDER_STANDING_HKEY 
		, CASE WHEN DATE_OF_SERVICE_FROM is null then '-1' 
			WHEN DATE_OF_SERVICE_FROM < '1901-01-01' then '-2' 
			WHEN DATE_OF_SERVICE_FROM > '2099-12-31' then '-3' 
			ELSE regexp_replace( DATE_OF_SERVICE_FROM, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                         PRESCRIPTION_FILL_DATE_KEY 
		, CASE WHEN DATE_OF_SERVICE_TO is null then '-1' 
			WHEN DATE_OF_SERVICE_TO < '1901-01-01' then '-2' 
			WHEN DATE_OF_SERVICE_TO > '2099-12-31' then '-3' 
			ELSE regexp_replace( DATE_OF_SERVICE_TO, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                         PRESCRIPTION_THRU_DATE_KEY 
		, CASE WHEN  LINE_STATUS_CODE  is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(LINE_STATUS_CODE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, CASE WHEN  INVOICE_STATUS is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(INVOICE_STATUS as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                             as                 INVOICE_HEADER_CURRENT_STATUS_HKEY 
		, CASE WHEN INTEREST_ACCRUAL_DATE is null then '-1' 
			WHEN INTEREST_ACCRUAL_DATE < '1901-01-01' then '-2' 
			WHEN INTEREST_ACCRUAL_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( INTEREST_ACCRUAL_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                          INTEREST_ACCRUAL_DATE_KEY 
		, CASE WHEN ADJUDICATION_STATUS_DATE is null then '-1' 
			WHEN ADJUDICATION_STATUS_DATE < '1901-01-01' then '-2' 
			WHEN ADJUDICATION_STATUS_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( ADJUDICATION_STATUS_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                          BWC_ADJUDICATION_DATE_KEY 
		, CASE WHEN PAYMENT_DATE is null then '-1' 
			WHEN PAYMENT_DATE < '1901-01-01' then '-2' 
			WHEN PAYMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PAYMENT_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                                      PAID_DATE_KEY 
		, CASE WHEN nullif(array_to_string(array_construct_compact( CLM_TYP_CD, CHNG_OVR_IND, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD ),''), '') is NULL 
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
		, MD5( '99999' )              as               HEALTHCARE_AUTHORIZATION_STATUS_HKEY 
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
		, CASE WHEN nullif(array_to_string(array_construct_compact( POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND ),''), '') is NULL  
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
		, CASE WHEN PRIOR_AUTH_EFFECTIVE_DATE is null then '-1' 
			WHEN PRIOR_AUTH_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN PRIOR_AUTH_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PRIOR_AUTH_EFFECTIVE_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                      PRIOR_AUTH_EFFECTIVE_DATE_KEY 
		, CASE WHEN PRIOR_AUTH_END_DATE is null then '-1' 
			WHEN PRIOR_AUTH_END_DATE < '1901-01-01' then '-2' 
			WHEN PRIOR_AUTH_END_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PRIOR_AUTH_END_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                            PRIOR_AUTH_END_DATE_KEY 
		, CASE WHEN WRNT_DATE is null then '-1' 
			WHEN WRNT_DATE < '1901-01-01' then '-2' 
			WHEN WRNT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( WRNT_DATE, '[^0-9]+', '') 
				END::INTEGER                                        
                                                             as                          ORIGINAL_WARRANT_DATE_KEY 
		, WRNT_NO                                            as                            ORIGINAL_WARRANT_NUMBER 
		, PRIOR_AUTHORIZATION_NUMBER                         as                         PRIOR_AUTHORIZATION_NUMBER 
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER 
		, PRESCRIPTION_NUMBER                                as                                PRESCRIPTION_NUMBER 
		, PRESCRIPTION_REFILL_NUMBER                         as                         PRESCRIPTION_REFILL_NUMBER 
		, PBM_BILL_DOCUMENT_NUMBER                           as                           PBM_BILL_DOCUMENT_NUMBER 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, UNITS_OF_SERVICE                                   as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT 
		, PAID_UNIT_QUANTITY                                 as                   LINE_UNITS_OF_PAID_SERVICE_COUNT 
		, BILLED_AMOUNT                                      as                        LINE_PROVIDER_BILLED_AMOUNT 
		, NTWK_BILLED_AMT                                    as                            LINE_PBM_ALLOWED_AMOUNT 
		, FEE_SCHED_AMOUNT                                   as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT 
		, CALC_AMOUNT                                        as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT 
		, APPROVED_AMOUNT                                    as                           LINE_BWC_APPROVED_AMOUNT 
		, INTEREST_AMOUNT                                    as                               LINE_INTEREST_AMOUNT 
		, (NVL(APPROVED_AMOUNT,0) + NVL(INTEREST_AMOUNT,0))  as                             LINE_REIMBURSED_AMOUNT 
		, PBM_ADMINISTRATIVE_FEE_AMOUNT                      as                      PBM_ADMINISTRATIVE_FEE_AMOUNT 
		, DRUG_REBATE_AMOUNT                                 as                                 DRUG_REBATE_AMOUNT 
		, PBM_INCENTIVE_FEE_AMOUNT                           as                           PBM_INCENTIVE_FEE_AMOUNT 
		, PBM_SUBMITTED_INGREDIENT_COST_AMOUNT               as               PBM_SUBMITTED_INGREDIENT_COST_AMOUNT 
		, PBM_APPROVED_INGREDIENT_COST_AMOUNT                as                PBM_APPROVED_INGREDIENT_COST_AMOUNT 
		, PBM_SUBMITTED_DISPENSING_FEE_AMOUNT                as                PBM_SUBMITTED_DISPENSING_FEE_AMOUNT 
		, PBM_APPROVED_DISPENSING_FEE_AMOUNT                 as                 PBM_APPROVED_DISPENSING_FEE_AMOUNT 
		, PBM_APPROVED_SALES_TAX_AMOUNT                      as                      PBM_APPROVED_SALES_TAX_AMOUNT 
		, PBM_SUBMITTED_SALES_TAX_AMOUNT                     as                     PBM_SUBMITTED_SALES_TAX_AMOUNT  
		, DRUG_DAYS_SUPPLIED                                 as                                 DRUG_DAYS_SUPPLIED 
		, DRUG_QUANTITY                                      as                                      DRUG_QUANTITY 
		, AWP_DRUG_AMOUNT                                    as                                    AWP_DRUG_AMOUNT 
		, IW_BRAND_NAME_CO_PAY_AMOUNT                        as                        IW_BRAND_NAME_CO_PAY_AMOUNT 
		, PBM_SUBMITTED_UCR_AMOUNT                           as                           PBM_SUBMITTED_UCR_AMOUNT 
		, GROSS_AMOUNT_DUE                                   as                                   GROSS_AMOUNT_DUE 
		, BATCH_NUMBER                                       as                              BUSINESS_BATCH_NUMBER 
		, INVOICE_TYPE_DESC                                  as                                  INVOICE_TYPE_DESC 
		, MCO_NUMBER                                         as                                 INVOICE_MCO_NUMBER 
		, SERVICING_PEACH_NUMBER                             as                     INVOICE_SERVICING_PEACH_NUMBER 
		, PAYTO_PEACH_NUMBER                                 as                         INVOICE_PAYTO_PEACH_NUMBER 
		, PROCEDURE_CODE                                     as                             INVOICE_PROCEDURE_CODE 
		, NATIONAL_DRUG_CODE                                 as                                 NATIONAL_DRUG_CODE 
		, PLACE_OF_SERVICE_CODE                              as                      INVOICE_PLACE_OF_SERVICE_CODE 
		, REFERRING_PEACH_NUMBER                             as                     INVOICE_REFERRING_PEACH_NUMBER 
		, RELATEDNESS_ICD                                    as                               RELATEDNESS_ICD_CODE 
		, DIAGNOSIS_CODE                                     as                             INVOICE_DIAGNOSIS_CODE 
		, DIAGNOSIS1                                         as                                 INVOICE_DIAGNOSIS1 
		, DIAGNOSIS2                                         as                                 INVOICE_DIAGNOSIS2 
		, DIAGNOSIS3                                         as                                 INVOICE_DIAGNOSIS3 
		, DIAGNOSIS4                                         as                                 INVOICE_DIAGNOSIS4 
		, PAID_MCO_NUMBER                                    as                                   INVOICE_PAID_MCO 
		, CUSTOMER_NUMBER                                    as                            INVOICE_CUSTOMER_NUMBER 
		, INVOICE_TYPE                                       as                                       INVOICE_TYPE 
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
		, CLIENT_PRICING                                     as                                     CLIENT_PRICING 
		, PRICE_TYPE                                         as                                         PRICE_TYPE 
		, SERVICE_LEVEL_CODE                                 as                                 SERVICE_LEVEL_CODE 
		, PHARMACIST_SERVICE_CODE                            as                            PHARMACIST_SERVICE_CODE 
		, SERVICE_RESULT_CODE                                as                                SERVICE_RESULT_CODE 
		, SERVICE_REASON_CODE                                as                                SERVICE_REASON_CODE 
		, SUBMITTED_DAW_CODE                                 as                                 SUBMITTED_DAW_CODE 
		, SUBMISSION_CLARIFICATION_CODE                      as                      SUBMISSION_CLARIFICATION_CODE 
		, PBM_BENEFIT_PLAN_TYPE_DESC                         as                         PBM_BENEFIT_PLAN_TYPE_DESC 
		, PHARM_SPECIAL_PROGRAM_DESC                         as                         PHARM_SPECIAL_PROGRAM_DESC 
		, PBM_ORIGINATION_TYPE_DESC                          as                          PBM_ORIGINATION_TYPE_DESC 
		, PBM_LOCK_IN_IND                                    as                                    PBM_LOCK_IN_IND 
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD 
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD 
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD 
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND 
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		, PRESCRIBING_PHYISCIAN                              as                              PRESCRIBING_PHYISCIAN 
		, DATE_OF_SERVICE_FROM                               as                               DATE_OF_SERVICE_FROM 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
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
		from SRC_INVOICE
            ),
LOGIC_PRESCRIB_PRVDR as ( SELECT 
		  PROVIDER_HKEY                                      as                          PRESCRIBING_PROVIDER_HKEY 
		, PROVIDER_PEACH_NUMBER                              as               PRESCRIB_PRVDR_PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as               PRESCRIB_PRVDR_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                     PRESCRIB_PRVDR_RECORD_END_DATE 
		from SRC_PRESCRIB_PRVDR
            ),
LOGIC_NDC as ( SELECT 
		  NDC_GPI_HKEY                                       as                                       NDC_GPI_HKEY 
		, NDC_11_CODE                                        as                                        NDC_11_CODE 
		, RECORD_EFFECTIVE_DATE                              as                          NDC_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                NDC_RECORD_END_DATE 
		, MILLIGRAMS_EQUIVALENCE_PER_UNIT                    as                    MILLIGRAMS_EQUIVALENCE_PER_UNIT 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_NDC
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
		  PROVIDER_HKEY                                      as                             PHARMACY_PROVIDER_HKEY 
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
LOGIC_R_ICD as ( SELECT 
		  ICD_HKEY                                           as                           PBM_RELATEDNESS_ICD_HKEY 
		, ICD_CODE                                           as                                     R_ICD_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                               R_ICD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                     R_ICD_END_DATE  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_R_ICD
            ),
LOGIC_P_ICD as ( SELECT 
		  ICD_HKEY                                           as                                 PRINCIPAL_ICD_HKEY 
		, ICD_CODE                                           as                                     P_ICD_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                               P_ICD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                     P_ICD_END_DATE  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_P_ICD
            ),
LOGIC_ICD1 as ( SELECT 
		  ICD_HKEY                                           as                      PHARMACY_SUBMITTED_ICD_1_HKEY 
		, ICD_CODE                                           as                                      ICD1_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD1_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD1_END_DATE  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_ICD1
            ),
LOGIC_ICD2 as ( SELECT 
		  ICD_HKEY                                           as                      PHARMACY_SUBMITTED_ICD_2_HKEY 
		, ICD_CODE                                           as                                      ICD2_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD2_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD2_END_DATE 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_ICD2
            ),
LOGIC_ICD3 as ( SELECT 
		  ICD_HKEY                                           as                      PHARMACY_SUBMITTED_ICD_3_HKEY 
		, ICD_CODE                                           as                                      ICD3_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD3_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD3_END_DATE  
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		from SRC_ICD3
            ),
LOGIC_ICD4 as ( SELECT 
		  ICD_HKEY                                           as                      PHARMACY_SUBMITTED_ICD_4_HKEY 
		, ICD_CODE                                           as                                      ICD4_ICD_CODE 
		, RECORD_EFFECTIVE_DATE                              as                                ICD4_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                      ICD4_END_DATE  
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
            ),
LOGIC_CLM as ( SELECT 
		  CLAIM_NUMBER                                       as                                   CLM_CLAIM_NUMBER 
		from SRC_CLM
            ),
LOGIC_R_PRVDR as ( SELECT 
		  PROVIDER_PEACH_NUMBER                              as                      R_PRVDR_PROVIDER_PEACH_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                      R_PRVDR_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                            R_PRVDR_RECORD_END_DATE 
		from SRC_R_PRVDR
            )

---- RENAME LAYER ----
,

RENAME_INVOICE as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, PRESCRIPTION_BILL_HKEY                             as                             PRESCRIPTION_BILL_HKEY
		, PRESCRIPTION_WRITTEN_DATE_KEY                      as                      PRESCRIPTION_WRITTEN_DATE_KEY
		, PBM_ADJUDICATION_DATE_KEY                          as                          PBM_ADJUDICATION_DATE_KEY
		, DRUG_REBATE_RECEIVED_DATE_KEY                      as                      DRUG_REBATE_RECEIVED_DATE_KEY
		, PBM_PRICING_METHOD_HKEY                            as                            PBM_PRICING_METHOD_HKEY
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, PHARMACY_STANDING_HKEY                             as                             PHARMACY_STANDING_HKEY
		, PRESCRIBING_PROVIDER_STANDING_HKEY                 as                 PRESCRIBING_PROVIDER_STANDING_HKEY
		, PRESCRIPTION_FILL_DATE_KEY                         as                         PRESCRIPTION_FILL_DATE_KEY
		, PRESCRIPTION_THRU_DATE_KEY                         as                         PRESCRIPTION_THRU_DATE_KEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
	    , CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
--		, HEALTHCARE_AUTHORIZATION_STATUS_HKEY               as               HEALTHCARE_AUTHORIZATION_STATUS_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, PRIOR_AUTH_EFFECTIVE_DATE_KEY                      as                      PRIOR_AUTH_EFFECTIVE_DATE_KEY
		, PRIOR_AUTH_END_DATE_KEY                            as                            PRIOR_AUTH_END_DATE_KEY
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER
		, PRIOR_AUTHORIZATION_NUMBER                         as                         PRIOR_AUTHORIZATION_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, PRESCRIPTION_NUMBER                                as                                PRESCRIPTION_NUMBER
		, PRESCRIPTION_REFILL_NUMBER                         as                         PRESCRIPTION_REFILL_NUMBER
		, PBM_BILL_DOCUMENT_NUMBER                           as                           PBM_BILL_DOCUMENT_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER		
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_PBM_ALLOWED_AMOUNT                            as                            LINE_PBM_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, PBM_ADMINISTRATIVE_FEE_AMOUNT                      as                      PBM_ADMINISTRATIVE_FEE_AMOUNT
		, DRUG_REBATE_AMOUNT                                 as                                 DRUG_REBATE_AMOUNT
		, PBM_INCENTIVE_FEE_AMOUNT                           as                           PBM_INCENTIVE_FEE_AMOUNT
		, PBM_SUBMITTED_INGREDIENT_COST_AMOUNT               as               PBM_SUBMITTED_INGREDIENT_COST_AMOUNT
		, PBM_APPROVED_INGREDIENT_COST_AMOUNT                as                PBM_APPROVED_INGREDIENT_COST_AMOUNT
		, PBM_SUBMITTED_DISPENSING_FEE_AMOUNT                as                PBM_SUBMITTED_DISPENSING_FEE_AMOUNT
		, PBM_APPROVED_DISPENSING_FEE_AMOUNT                 as                 PBM_APPROVED_DISPENSING_FEE_AMOUNT
		, PBM_APPROVED_SALES_TAX_AMOUNT                      as                      PBM_APPROVED_SALES_TAX_AMOUNT
		, PBM_SUBMITTED_SALES_TAX_AMOUNT                     as                     PBM_SUBMITTED_SALES_TAX_AMOUNT
		, DRUG_DAYS_SUPPLIED                                 as                                 DRUG_DAYS_SUPPLIED
		, DRUG_QUANTITY                                      as                                      DRUG_QUANTITY
		, AWP_DRUG_AMOUNT                                    as                                    AWP_DRUG_AMOUNT
		, IW_BRAND_NAME_CO_PAY_AMOUNT                        as                        IW_BRAND_NAME_CO_PAY_AMOUNT
		, PBM_SUBMITTED_UCR_AMOUNT                           as                           PBM_SUBMITTED_UCR_AMOUNT
		, GROSS_AMOUNT_DUE                                   as                                   GROSS_AMOUNT_DUE
		, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER
		, INVOICE_TYPE_DESC                                  as                                  INVOICE_TYPE_DESC
		, INVOICE_MCO_NUMBER                                 as                                 INVOICE_MCO_NUMBER
		, INVOICE_SERVICING_PEACH_NUMBER                     as                     INVOICE_SERVICING_PEACH_NUMBER
		, INVOICE_PAYTO_PEACH_NUMBER                         as                         INVOICE_PAYTO_PEACH_NUMBER
		, INVOICE_PROCEDURE_CODE                             as                             INVOICE_PROCEDURE_CODE
		, NATIONAL_DRUG_CODE                                 as                                 NATIONAL_DRUG_CODE
		, INVOICE_PLACE_OF_SERVICE_CODE                      as                      INVOICE_PLACE_OF_SERVICE_CODE
		, INVOICE_REFERRING_PEACH_NUMBER                     as                     INVOICE_REFERRING_PEACH_NUMBER
		, RELATEDNESS_ICD_CODE                               as                               RELATEDNESS_ICD_CODE
		, INVOICE_DIAGNOSIS_CODE                             as                             INVOICE_DIAGNOSIS_CODE
		, INVOICE_DIAGNOSIS1                                 as                                 INVOICE_DIAGNOSIS1
		, INVOICE_DIAGNOSIS2                                 as                                 INVOICE_DIAGNOSIS2
		, INVOICE_DIAGNOSIS3                                 as                                 INVOICE_DIAGNOSIS3
		, INVOICE_DIAGNOSIS4                                 as                                 INVOICE_DIAGNOSIS4
		, INVOICE_PAID_MCO                                   as                                   INVOICE_PAID_MCO
		, INVOICE_CUSTOMER_NUMBER                            as                            INVOICE_CUSTOMER_NUMBER
		, INVOICE_TYPE                                       as                                       INVOICE_TYPE
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
		, CLIENT_PRICING                                     as                                     CLIENT_PRICING
		, PRICE_TYPE                                         as                                         PRICE_TYPE
		, SERVICE_LEVEL_CODE                                 as                                 SERVICE_LEVEL_CODE
		, PHARMACIST_SERVICE_CODE                            as                            PHARMACIST_SERVICE_CODE
		, SERVICE_RESULT_CODE                                as                                SERVICE_RESULT_CODE
		, SERVICE_REASON_CODE                                as                                SERVICE_REASON_CODE
		, SUBMITTED_DAW_CODE                                 as                                 SUBMITTED_DAW_CODE
		, SUBMISSION_CLARIFICATION_CODE                      as                      SUBMISSION_CLARIFICATION_CODE
		, PBM_BENEFIT_PLAN_TYPE_DESC                         as                         PBM_BENEFIT_PLAN_TYPE_DESC
		, PHARM_SPECIAL_PROGRAM_DESC                         as                         PHARM_SPECIAL_PROGRAM_DESC
		, PBM_ORIGINATION_TYPE_DESC                          as                          PBM_ORIGINATION_TYPE_DESC
		, PBM_LOCK_IN_IND                                    as                                    PBM_LOCK_IN_IND
		, CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PRESCRIBING_PHYISCIAN                              as                              PRESCRIBING_PHYISCIAN
		, DATE_OF_SERVICE_FROM                               as                               DATE_OF_SERVICE_FROM 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
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

				FROM     LOGIC_INVOICE   ), 
RENAME_PRESCRIB_PRVDR as ( SELECT 
		  PRESCRIBING_PROVIDER_HKEY                          as                          PRESCRIBING_PROVIDER_HKEY
		, PRESCRIB_PRVDR_PROVIDER_PEACH_NUMBER               as               PRESCRIB_PRVDR_PROVIDER_PEACH_NUMBER
		, PRESCRIB_PRVDR_RECORD_EFFECTIVE_DATE               as               PRESCRIB_PRVDR_RECORD_EFFECTIVE_DATE
		, PRESCRIB_PRVDR_RECORD_END_DATE                     as                     PRESCRIB_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_PRESCRIB_PRVDR   ), 
RENAME_NDC as ( SELECT 
		  NDC_GPI_HKEY                                       as                                       NDC_GPI_HKEY
		, NDC_11_CODE                                        as                                        NDC_11_CODE
		, NDC_RECORD_EFFECTIVE_DATE                          as                          NDC_RECORD_EFFECTIVE_DATE
		, NDC_RECORD_END_DATE                                as                                NDC_RECORD_END_DATE
		, MILLIGRAMS_EQUIVALENCE_PER_UNIT                    as                    MILLIGRAMS_EQUIVALENCE_PER_UNIT 
		, CURRENT_RECORD_IND                                 as                             NDC_CURRENT_RECORD_IND 
				FROM     LOGIC_NDC   ), 
RENAME_NTWK as ( SELECT 
		  SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY
		, NTWK_NETWORK_NUMBER                                as                                NTWK_NETWORK_NUMBER
		, NTWK_RECORD_EFFECTIVE_DATE                         as                         NTWK_RECORD_EFFECTIVE_DATE
		, NTWK_RECORD_END_DATE                               as                               NTWK_RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                            NTWK_CURRENT_RECORD_IND
				FROM     LOGIC_NTWK   ), 
RENAME_S_PRVDR as ( SELECT 
		  PHARMACY_PROVIDER_HKEY                             as                             PHARMACY_PROVIDER_HKEY
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
RENAME_R_ICD as ( SELECT 
		  PBM_RELATEDNESS_ICD_HKEY                           as                           PBM_RELATEDNESS_ICD_HKEY
		, R_ICD_ICD_CODE                                     as                                     R_ICD_ICD_CODE
		, R_ICD_EFFECTIVE_DATE                               as                               R_ICD_EFFECTIVE_DATE
		, R_ICD_END_DATE                                     as                                     R_ICD_END_DATE
		, CURRENT_RECORD_IND                                 as                           R_ICD_CURRENT_RECORD_IND
				FROM     LOGIC_R_ICD   ), 
RENAME_P_ICD as ( SELECT 
		  PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
		, P_ICD_ICD_CODE                                     as                                     P_ICD_ICD_CODE
		, P_ICD_EFFECTIVE_DATE                               as                               P_ICD_EFFECTIVE_DATE
		, P_ICD_END_DATE                                     as                                     P_ICD_END_DATE
		, CURRENT_RECORD_IND                                 as                           P_ICD_CURRENT_RECORD_IND
				FROM     LOGIC_P_ICD   ), 
RENAME_ICD1 as ( SELECT 
		  PHARMACY_SUBMITTED_ICD_1_HKEY                      as                      PHARMACY_SUBMITTED_ICD_1_HKEY
		, ICD1_ICD_CODE                                      as                                      ICD1_ICD_CODE
		, ICD1_EFFECTIVE_DATE                                as                                ICD1_EFFECTIVE_DATE
		, ICD1_END_DATE                                      as                                      ICD1_END_DATE
		, CURRENT_RECORD_IND                                 as                            ICD1_CURRENT_RECORD_IND
				FROM     LOGIC_ICD1   ), 
RENAME_ICD2 as ( SELECT 
		  PHARMACY_SUBMITTED_ICD_2_HKEY                      as                      PHARMACY_SUBMITTED_ICD_2_HKEY
		, ICD2_ICD_CODE                                      as                                      ICD2_ICD_CODE
		, ICD2_EFFECTIVE_DATE                                as                                ICD2_EFFECTIVE_DATE
		, ICD2_END_DATE                                      as                                      ICD2_END_DATE
		, CURRENT_RECORD_IND                                 as                            ICD2_CURRENT_RECORD_IND
				FROM     LOGIC_ICD2   ), 
RENAME_ICD3 as ( SELECT 
		  PHARMACY_SUBMITTED_ICD_3_HKEY                      as                      PHARMACY_SUBMITTED_ICD_3_HKEY
		, ICD3_ICD_CODE                                      as                                      ICD3_ICD_CODE
		, ICD3_EFFECTIVE_DATE                                as                                ICD3_EFFECTIVE_DATE
		, ICD3_END_DATE                                      as                                      ICD3_END_DATE 
		, CURRENT_RECORD_IND                                 as                            ICD3_CURRENT_RECORD_IND
				FROM     LOGIC_ICD3   ), 
RENAME_ICD4 as ( SELECT 
		  PHARMACY_SUBMITTED_ICD_4_HKEY                      as                      PHARMACY_SUBMITTED_ICD_4_HKEY
		, ICD4_ICD_CODE                                      as                                      ICD4_ICD_CODE
		, ICD4_EFFECTIVE_DATE                                as                                ICD4_EFFECTIVE_DATE
		, ICD4_END_DATE                                      as                                      ICD4_END_DATE
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
				FROM     LOGIC_IW   ), 
RENAME_CLM as ( SELECT 
		  CLM_CLAIM_NUMBER                                   as                                   CLM_CLAIM_NUMBER 
				FROM     LOGIC_CLM   ), 
RENAME_R_PRVDR as ( SELECT 
		  R_PRVDR_PROVIDER_PEACH_NUMBER                      as                      R_PRVDR_PROVIDER_PEACH_NUMBER
		, R_PRVDR_RECORD_EFFECTIVE_DATE                      as                      R_PRVDR_RECORD_EFFECTIVE_DATE
		, R_PRVDR_RECORD_END_DATE                            as                            R_PRVDR_RECORD_END_DATE 
				FROM     LOGIC_R_PRVDR   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_INVOICE                        as ( SELECT * from    RENAME_INVOICE 
                                            WHERE INVOICE_TYPE = 'PS'  ),
FILTER_NTWK                           as ( SELECT * from    RENAME_NTWK   ),
FILTER_PRESCRIB_PRVDR                 as ( SELECT * from    RENAME_PRESCRIB_PRVDR   ),
FILTER_S_PRVDR                        as ( SELECT * from    RENAME_S_PRVDR   ),
FILTER_P_PRVDR                        as ( SELECT * from    RENAME_P_PRVDR   ),
FILTER_R_PRVDR                        as ( SELECT * from    RENAME_R_PRVDR   ),
FILTER_R_ICD                          as ( SELECT * from    RENAME_R_ICD   ),
FILTER_P_ICD                          as ( SELECT * from    RENAME_P_ICD   ),
FILTER_ICD1                           as ( SELECT * from    RENAME_ICD1   ),
FILTER_ICD2                           as ( SELECT * from    RENAME_ICD2   ),
FILTER_ICD3                           as ( SELECT * from    RENAME_ICD3   ),
FILTER_ICD4                           as ( SELECT * from    RENAME_ICD4   ),
FILTER_P_NTWK                         as ( SELECT * from    RENAME_P_NTWK   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_NDC                            as ( SELECT * from    RENAME_NDC   ),

---- JOIN LAYER ----

INVOICE as ( SELECT *
				FROM  FILTER_INVOICE
				LEFT JOIN FILTER_PRESCRIB_PRVDR ON  coalesce( FILTER_INVOICE.PRESCRIBING_PHYISCIAN, '99999999999') =  FILTER_PRESCRIB_PRVDR.PRESCRIB_PRVDR_PROVIDER_PEACH_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN PRESCRIB_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( PRESCRIB_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_S_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_SERVICING_PEACH_NUMBER, '99999999999') =  FILTER_S_PRVDR.S_PRVDR_PROVIDER_PEACH_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN S_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( S_PRVDR_RECORD_END_DATE, '2099-12-31') 
						        LEFT JOIN FILTER_NTWK ON  coalesce( FILTER_INVOICE.INVOICE_MCO_NUMBER, '00000') =  FILTER_NTWK.NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN NTWK_RECORD_EFFECTIVE_DATE AND coalesce( NTWK_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_PAYTO_PEACH_NUMBER,'99999999999') =  FILTER_P_PRVDR.P_PRVDR_PROVIDER_PEACH_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN P_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( P_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_R_PRVDR ON  coalesce( FILTER_INVOICE.INVOICE_REFERRING_PEACH_NUMBER, '99999999999') =  FILTER_R_PRVDR.R_PRVDR_PROVIDER_PEACH_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN R_PRVDR_RECORD_EFFECTIVE_DATE AND coalesce( R_PRVDR_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_R_ICD ON  coalesce( FILTER_INVOICE.RELATEDNESS_ICD_CODE,  'UNK') =  FILTER_R_ICD.R_ICD_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN R_ICD_EFFECTIVE_DATE AND coalesce( R_ICD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_ICD ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS_CODE,  'UNK') =  FILTER_P_ICD.P_ICD_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN P_ICD_EFFECTIVE_DATE AND coalesce( P_ICD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD1 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS1, 'UNK') =  FILTER_ICD1.ICD1_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN ICD1_EFFECTIVE_DATE AND coalesce( ICD1_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD2 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS2, 'UNK') =  FILTER_ICD2.ICD2_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN ICD2_EFFECTIVE_DATE AND coalesce( ICD2_END_DATE, '2099-12-31')
								LEFT JOIN FILTER_ICD3 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS3, 'UNK') =  FILTER_ICD3.ICD3_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN ICD3_EFFECTIVE_DATE AND coalesce( ICD3_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_ICD4 ON  coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS4, 'UNK') =  FILTER_ICD4.ICD4_ICD_CODE AND INVOICE_RECEIPT_DATE BETWEEN ICD4_EFFECTIVE_DATE AND coalesce( ICD4_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_P_NTWK ON  coalesce( FILTER_INVOICE.INVOICE_PAID_MCO, '00000') =  FILTER_P_NTWK.P_NTWK_NETWORK_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN P_NTWK_RECORD_EFFECTIVE_DATE AND coalesce( P_NTWK_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_IW ON  coalesce( FILTER_INVOICE.INVOICE_CUSTOMER_NUMBER, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND INVOICE_RECEIPT_DATE BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_CLM ON  coalesce( FILTER_INVOICE.CLAIM_NUMBER, 'UNK') =  FILTER_CLM.CLM_CLAIM_NUMBER 
								LEFT JOIN FILTER_NDC ON  coalesce( FILTER_INVOICE.NATIONAL_DRUG_CODE, '00000000000') =  FILTER_NDC.NDC_11_CODE AND DATE_OF_SERVICE_FROM BETWEEN NDC_RECORD_EFFECTIVE_DATE AND coalesce( NDC_RECORD_END_DATE, '2099-12-31') 
								),
                                
-- ETL join layer to handle NDC & ICDs that are outside of date range MD5('-2222') 
								                                
ETL_SRT AS (SELECT FILTER_INVOICE.*
            , FILTER_NDC1.NDC_11_CODE AS FILTER_NDC1_NDC_11_CODE
            , FILTER_R_ICD1.R_ICD_ICD_CODE AS FILTER_R_ICD1_R_ICD_ICD_CODE
            , FILTER_P_ICD1.P_ICD_ICD_CODE AS FILTER_P_ICD1_P_ICD_ICD_CODE
            , FILTER_ICD1_SRT.ICD1_ICD_CODE AS FILTER_ICD1_SRT_ICD1_ICD_CODE
            , FILTER_ICD2_SRT.ICD2_ICD_CODE AS FILTER_ICD2_SRT_ICD2_ICD_CODE
            , FILTER_ICD3_SRT.ICD3_ICD_CODE AS FILTER_ICD3_SRT_ICD3_ICD_CODE
            , FILTER_ICD4_SRT.ICD4_ICD_CODE AS FILTER_ICD4_SRT_ICD4_ICD_CODE
			, FILTER_NTWK1.NTWK_NETWORK_NUMBER AS FILTER_NTWK1_NTWK_NETWORK_NUMBER
			, FILTER_P_NTWK1.P_NTWK_NETWORK_NUMBER AS FILTER_P_NTWK1_P_NTWK_NETWORK_NUMBER
            FROM INVOICE FILTER_INVOICE
            LEFT JOIN FILTER_R_ICD FILTER_R_ICD1 ON coalesce( FILTER_INVOICE.RELATEDNESS_ICD_CODE,  'Z') =  FILTER_R_ICD1.R_ICD_ICD_CODE AND FILTER_R_ICD1.R_ICD_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_P_ICD FILTER_P_ICD1 ON coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS_CODE,  'Z') =  FILTER_P_ICD1.P_ICD_ICD_CODE AND FILTER_P_ICD1.P_ICD_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD1 FILTER_ICD1_SRT ON coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS1,  'Z') =  FILTER_ICD1_SRT.ICD1_ICD_CODE AND FILTER_ICD1_SRT.ICD1_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD2 FILTER_ICD2_SRT ON coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS2,  'Z') =  FILTER_ICD2_SRT.ICD2_ICD_CODE AND FILTER_ICD2_SRT.ICD2_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD3 FILTER_ICD3_SRT ON coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS3,  'Z') =  FILTER_ICD3_SRT.ICD3_ICD_CODE AND FILTER_ICD3_SRT.ICD3_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_ICD4 FILTER_ICD4_SRT ON coalesce( FILTER_INVOICE.INVOICE_DIAGNOSIS4,  'Z') =  FILTER_ICD4_SRT.ICD4_ICD_CODE AND FILTER_ICD4_SRT.ICD4_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_NDC FILTER_NDC1 ON  coalesce( FILTER_INVOICE.NATIONAL_DRUG_CODE, 'Z') =  FILTER_NDC1.NDC_11_CODE AND FILTER_NDC1.NDC_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_NTWK FILTER_NTWK1 ON  coalesce( FILTER_INVOICE.INVOICE_MCO_NUMBER, 'Z') =  FILTER_NTWK1.NTWK_NETWORK_NUMBER AND FILTER_NTWK1.NTWK_CURRENT_RECORD_IND = 'Y' 
			LEFT JOIN FILTER_P_NTWK FILTER_P_NTWK1 ON coalesce( FILTER_INVOICE.INVOICE_PAID_MCO, 'Z') =  FILTER_P_NTWK1.P_NTWK_NETWORK_NUMBER AND FILTER_P_NTWK1.P_NTWK_CURRENT_RECORD_IND = 'Y' 
			)
SELECT 
		  MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, PRESCRIPTION_BILL_HKEY
		, PRESCRIPTION_WRITTEN_DATE_KEY
		, PBM_ADJUDICATION_DATE_KEY
		, DRUG_REBATE_RECEIVED_DATE_KEY
		, coalesce( PRESCRIBING_PROVIDER_HKEY, MD5( '-1111' )) as PRESCRIBING_PROVIDER_HKEY 
		, CASE WHEN NDC_11_CODE IS NOT NULL THEN NDC_GPI_HKEY
                WHEN FILTER_NDC1_NDC_11_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS NDC_GPI_HKEY 
		, coalesce( PBM_PRICING_METHOD_HKEY, MD5( '-1111' )) as PBM_PRICING_METHOD_HKEY
		, INVOICE_PROFILE_HKEY
		, CASE WHEN SUBMITTING_NETWORK_HKEY IS NOT NULL THEN SUBMITTING_NETWORK_HKEY
                WHEN FILTER_NTWK1_NTWK_NETWORK_NUMBER IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS SUBMITTING_NETWORK_HKEY 
		, BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY
		, coalesce( PHARMACY_PROVIDER_HKEY, MD5( '-1111' )) as PHARMACY_PROVIDER_HKEY 
	/*	, coalesce( PHARMACY_STANDING_HKEY, MD5( '-1111' )) as PHARMACY_STANDING_HKEY 
		, coalesce( PRESCRIBING_PROVIDER_STANDING_HKEY, MD5( '-1111' )) as PRESCRIBING_PROVIDER_STANDING_HKEY */
		, coalesce( PAY_TO_PROVIDER_HKEY, MD5( '-1111' )) as PAY_TO_PROVIDER_HKEY 
		, PRESCRIPTION_FILL_DATE_KEY
		, PRESCRIPTION_THRU_DATE_KEY
        , CASE WHEN R_ICD_ICD_CODE IS NOT NULL THEN PBM_RELATEDNESS_ICD_HKEY     
                WHEN FILTER_R_ICD1_R_ICD_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PBM_RELATEDNESS_ICD_HKEY
		, CASE WHEN P_ICD_ICD_CODE IS NOT NULL THEN PRINCIPAL_ICD_HKEY
                WHEN FILTER_P_ICD1_P_ICD_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PRINCIPAL_ICD_HKEY
		, CASE WHEN ICD1_ICD_CODE IS NOT NULL THEN PHARMACY_SUBMITTED_ICD_1_HKEY
                WHEN FILTER_ICD1_SRT_ICD1_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PHARMACY_SUBMITTED_ICD_1_HKEY
		, CASE WHEN ICD2_ICD_CODE IS NOT NULL THEN PHARMACY_SUBMITTED_ICD_2_HKEY
                WHEN FILTER_ICD2_SRT_ICD2_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PHARMACY_SUBMITTED_ICD_2_HKEY
		, CASE WHEN ICD3_ICD_CODE IS NOT NULL THEN PHARMACY_SUBMITTED_ICD_3_HKEY
                WHEN FILTER_ICD3_SRT_ICD3_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PHARMACY_SUBMITTED_ICD_3_HKEY
		, CASE WHEN ICD4_ICD_CODE IS NOT NULL THEN PHARMACY_SUBMITTED_ICD_4_HKEY
                WHEN FILTER_ICD4_SRT_ICD4_ICD_CODE IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PHARMACY_SUBMITTED_ICD_4_HKEY 
		, coalesce( INVOICE_LINE_ITEM_STATUS_HKEY, MD5( '-1111' )) as INVOICE_LINE_ITEM_STATUS_HKEY
		, coalesce( INVOICE_HEADER_CURRENT_STATUS_HKEY, MD5( '-1111' )) as INVOICE_HEADER_CURRENT_STATUS_HKEY
		, CASE WHEN PAY_TO_NETWORK_HKEY IS NOT NULL THEN PAY_TO_NETWORK_HKEY
                WHEN FILTER_P_NTWK1_P_NTWK_NETWORK_NUMBER IS NOT NULL THEN MD5('-2222')
        		ELSE MD5('-1111') END AS PAY_TO_NETWORK_HKEY 
		, INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY
		, coalesce( INJURED_WORKER_HKEY, MD5( '-1111' )) as INJURED_WORKER_HKEY
		, PAID_DATE_KEY
		, coalesce( CLAIM_TYPE_STATUS_HKEY, MD5( '-1111' )) as CLAIM_TYPE_STATUS_HKEY
		, coalesce( CLAIM_DETAIL_HKEY, MD5( '-1111' )) as CLAIM_DETAIL_HKEY
	/*	, HEALTHCARE_AUTHORIZATION_STATUS_HKEY */
		, POLICY_STANDING_HKEY
		, PRIOR_AUTH_EFFECTIVE_DATE_KEY
		, PRIOR_AUTH_END_DATE_KEY
		, ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER
		, PRIOR_AUTHORIZATION_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER
		, PRESCRIPTION_NUMBER
		, PRESCRIPTION_REFILL_NUMBER
		, PBM_BILL_DOCUMENT_NUMBER
		, CLAIM_NUMBER
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT
		, LINE_PBM_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT
		, PBM_ADMINISTRATIVE_FEE_AMOUNT
		, DRUG_REBATE_AMOUNT
		, PBM_INCENTIVE_FEE_AMOUNT
		, PBM_SUBMITTED_INGREDIENT_COST_AMOUNT
		, PBM_APPROVED_INGREDIENT_COST_AMOUNT
		, PBM_SUBMITTED_DISPENSING_FEE_AMOUNT
		, PBM_APPROVED_DISPENSING_FEE_AMOUNT
		, PBM_APPROVED_SALES_TAX_AMOUNT
		, PBM_SUBMITTED_SALES_TAX_AMOUNT
		, cast((DRUG_QUANTITY * MILLIGRAMS_EQUIVALENCE_PER_UNIT) /nullif(DRUG_DAYS_SUPPLIED,0) as numeric(32,4)) AS MILLIGRAM_EQUIVALENCE_PER_DAY
		, DRUG_DAYS_SUPPLIED
		, DRUG_QUANTITY::NUMERIC(32,4) AS DRUG_QUANTITY
		, AWP_DRUG_AMOUNT::NUMERIC(32,5) AS AWP_DRUG_AMOUNT
		, IW_BRAND_NAME_CO_PAY_AMOUNT
		, PBM_SUBMITTED_UCR_AMOUNT
		, GROSS_AMOUNT_DUE
		, CURRENT_TIMESTAMP() AS LOAD_DATETIME
		, 'CAM' AS PRIMARY_SOURCE_SYSTEM
		, BUSINESS_BATCH_NUMBER 
from ETL_SRT
      );
    