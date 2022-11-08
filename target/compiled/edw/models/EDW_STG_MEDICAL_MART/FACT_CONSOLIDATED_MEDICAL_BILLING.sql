

---- SRC LAYER ----
WITH
SRC_ASC as ( SELECT *     from     EDW_STG_MEDICAL_MART.FACT_PROFESSIONAL_ASC_BILLING ),
SRC_FNCL_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_PBM as ( SELECT *     from     EDW_STG_MEDICAL_MART.FACT_PRESCRIPTION_BILLING ),
SRC_HOUT as ( SELECT *     from     EDW_STG_MEDICAL_MART.FACT_HOSPITAL_OUTPATIENT_BILLING ),
SRC_HIN as ( SELECT *     from     EDW_STG_MEDICAL_MART.FACT_HOSPITAL_INPATIENT_BILLING ),
SRC_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
//SRC_ASC as ( SELECT *     from     FACT_PROFESSIONAL_ASC_BILLING) ,
//SRC_FNCL_NTWK as ( SELECT *     from     DIM_NETWORK) ,
//SRC_PBM as ( SELECT *     from     FACT_PRESCRIPTION_BILLING) ,
//SRC_HOUT as ( SELECT *     from     FACT_HOSPITAL_OUTPATIENT_BILLING) ,
//SRC_HIN as ( SELECT *     from     FACT_HOSPITAL_INPATIENT_BILLING) ,
//SRC_NTWK as ( SELECT *     from     DIM_NETWORK) ,

---- LOGIC LAYER ----

LOGIC_ASC as ( SELECT 
		  TRIM(MEDICAL_INVOICE_NUMBER)                       as                             MEDICAL_INVOICE_NUMBER 
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER 
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER 
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER 
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY 
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY 
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY 
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY 
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY 
		, SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY 
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY 
		, CPT_SERVICE_RENDERED_HKEY                          as                          CPT_SERVICE_RENDERED_HKEY 
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY 
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY 
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY 
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY 
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY 
		, REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY 
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY 
		, ICD_1_HKEY                                         as                                         ICD_1_HKEY 
		, ICD_2_HKEY                                         as                                         ICD_2_HKEY 
		, ICD_3_HKEY                                         as                                         ICD_3_HKEY 
		, ICD_4_HKEY                                         as                                         ICD_4_HKEY 
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, NULL                                               as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY 
	   	, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY 
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY 
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY 
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
        , CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY 
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY 
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY 
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER 
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
		from SRC_ASC
            ),
LOGIC_FNCL_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                       NETWORK_HKEY
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		, NETWORK_NUMBER                                     as                                     NETWORK_NUMBER 
		from SRC_FNCL_NTWK
            ),
LOGIC_PBM as ( SELECT 
		  TRIM(MEDICAL_INVOICE_NUMBER)                       as                             MEDICAL_INVOICE_NUMBER 
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER 
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER 
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER 
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY 
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY 
		, SUBMITTING_NETWORK_HKEY          					 as                            SUBMITTING_NETWORK_HKEY 
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY 
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY 
		, PHARMACY_PROVIDER_HKEY                             as                             PHARMACY_PROVIDER_HKEY 
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY 
		, NDC_GPI_HKEY                                       as                                       NDC_GPI_HKEY 
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))          as                              MODIFIER_SEQUENCE_HKEY		
		, PRESCRIPTION_FILL_DATE_KEY                         as                         PRESCRIPTION_FILL_DATE_KEY 
		, PRESCRIPTION_THRU_DATE_KEY                         as                         PRESCRIPTION_THRU_DATE_KEY 
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as                            REFERRING_PROVIDER_HKEY
		, PBM_RELATEDNESS_ICD_HKEY                           as                                 PRINCIPAL_ICD_HKEY 
		, PHARMACY_SUBMITTED_ICD_1_HKEY                      as                      PHARMACY_SUBMITTED_ICD_1_HKEY 
		, PHARMACY_SUBMITTED_ICD_2_HKEY                      as                      PHARMACY_SUBMITTED_ICD_2_HKEY 
		, PHARMACY_SUBMITTED_ICD_3_HKEY                      as                      PHARMACY_SUBMITTED_ICD_3_HKEY 
		, PHARMACY_SUBMITTED_ICD_4_HKEY                      as                      PHARMACY_SUBMITTED_ICD_4_HKEY 
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY         		
		, md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
))  as                          FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
        , PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY 
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY 
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY 
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY 
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY 
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY 
    	, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY 
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY 
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER 
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER 
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
		, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER 
		from SRC_PBM
            ),
LOGIC_HOUT as ( SELECT 
		  TRIM(MEDICAL_INVOICE_NUMBER)                       as                             MEDICAL_INVOICE_NUMBER 
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER 
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER 
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER 
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY 
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY 
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY 
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY 
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY 
		, SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY 
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY 
		, CPT_SERVICE_RENDERED_HKEY                          as                          CPT_SERVICE_RENDERED_HKEY 
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY 
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY 
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY 
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY 
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as 						   REFERRING_PROVIDER_HKEY		
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as 										ICD_1_HKEY
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as										    ICD_2_HKEY
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as 									    ICD_3_HKEY
	    , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as                                         ICD_4_HKEY	
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY 
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, NULL                                                as                FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY 
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY 
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY 
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY 
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY 
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY 
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY 
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY 
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER 
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT 
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT 
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT 
		, LINE_MCO_ALLOWED_AMOUNT                            as                            LINE_MCO_ALLOWED_AMOUNT 
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT 
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT 
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT 
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT 
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT 
		, INVOICE_BATCH_NUMBER                               as                               INVOICE_BATCH_NUMBER 
		from SRC_HOUT
            ),
LOGIC_HIN as ( SELECT 
		  TRIM(MEDICAL_INVOICE_NUMBER)                       as                             MEDICAL_INVOICE_NUMBER 
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER 
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER 
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER 
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER 
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY 
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY 
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY 
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY 
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY 
		, SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY 
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY 
		, CPT_SERVICE_RENDERED_HKEY                          as                          CPT_SERVICE_RENDERED_HKEY 
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY 
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY 
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY 
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY 
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY 
		, REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY 
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as 										ICD_1_HKEY
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as										    ICD_2_HKEY
        , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as 									    ICD_3_HKEY
	    , md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))           as                                         ICD_4_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY 
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, NULL                                               as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY 
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY 
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY 
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY 
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY 
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY 
	    , POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY 
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY 
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER 
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT 
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT 
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT 
		, LINE_MCO_ALLOWED_AMOUNT                            as                            LINE_MCO_ALLOWED_AMOUNT 
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT 
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT 
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT 
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT 
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT 
		, INVOICE_BATCH_NUMBER                               as                               INVOICE_BATCH_NUMBER 
		from SRC_HIN
            ),
LOGIC_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                       NETWORK_HKEY 
		, CURRENT_ACQUIRING_MCO_NUMBER                       as                       CURRENT_ACQUIRING_MCO_NUMBER 
		, NETWORK_NUMBER                                     as                                     NETWORK_NUMBER 
		from SRC_NTWK
            )

---- RENAME LAYER ----
,

RENAME_ASC as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY
		, CPT_SERVICE_RENDERED_HKEY                          as                   HEALTHCARE_SERVICE_RENDERED_HKEY
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY
		, REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
		, ICD_1_HKEY                                         as                                         ICD_1_HKEY
		, ICD_2_HKEY                                         as                                         ICD_2_HKEY
		, ICD_3_HKEY                                         as                                         ICD_3_HKEY
		, ICD_4_HKEY                                         as                                         ICD_4_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, FINANCIALLY_RESPONSIBLE_NETWORK_HKEY               as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_MCO_ALLOWED_AMOUNT                            as                        LINE_NETWORK_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER 
				FROM     LOGIC_ASC   ), 
RENAME_FNCL_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
		, NETWORK_NUMBER                                     as                           FNCL_NTWK_NETWORK_NUMBER 
				FROM     LOGIC_FNCL_NTWK   ), 
RENAME_PBM as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, PHARMACY_PROVIDER_HKEY                             as                              SERVICE_PROVIDER_HKEY
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY
		, NDC_GPI_HKEY                                       as                   HEALTHCARE_SERVICE_RENDERED_HKEY
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY
		, PRESCRIPTION_FILL_DATE_KEY                         as                      INVOICE_SERVICE_FROM_DATE_KEY
		, PRESCRIPTION_THRU_DATE_KEY                         as                        INVOICE_SERVICE_TO_DATE_KEY
		, PRESCRIPTION_FILL_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY
		, PRESCRIPTION_THRU_DATE_KEY                         as                           LINE_SERVICE_TO_DATE_KEY
		, REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
		, PHARMACY_SUBMITTED_ICD_1_HKEY                      as                                         ICD_1_HKEY
		, PHARMACY_SUBMITTED_ICD_2_HKEY                      as                                         ICD_2_HKEY
		, PHARMACY_SUBMITTED_ICD_3_HKEY                      as                                         ICD_3_HKEY
		, PHARMACY_SUBMITTED_ICD_4_HKEY                      as                                         ICD_4_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, FINANCIALLY_RESPONSIBLE_NETWORK_HKEY                 as                FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_PBM_ALLOWED_AMOUNT                            as                        LINE_NETWORK_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, BUSINESS_BATCH_NUMBER                              as                              BUSINESS_BATCH_NUMBER 
				FROM     LOGIC_PBM   ), 
RENAME_HOUT as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY
		, CPT_SERVICE_RENDERED_HKEY                          as                   HEALTHCARE_SERVICE_RENDERED_HKEY
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY
		, REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
		, ICD_1_HKEY                                         as                                         ICD_1_HKEY
		, ICD_2_HKEY                                         as                                         ICD_2_HKEY
		, ICD_3_HKEY                                         as                                         ICD_3_HKEY
		, ICD_4_HKEY                                         as                                         ICD_4_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY                
		, FINANCIALLY_RESPONSIBLE_NETWORK_HKEY               as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_MCO_ALLOWED_AMOUNT                            as                        LINE_NETWORK_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, INVOICE_BATCH_NUMBER                               as                              BUSINESS_BATCH_NUMBER 
				FROM     LOGIC_HOUT   ), 
RENAME_HIN as ( SELECT 
		  MEDICAL_INVOICE_NUMBER                             as                             MEDICAL_INVOICE_NUMBER
		, MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER               as               MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
		, MEDICAL_INVOICE_LINE_EXTENSION_NUMBER              as              MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
		, MEDICAL_INVOICE_LINE_VERSION_NUMBER                as                MEDICAL_INVOICE_LINE_VERSION_NUMBER
		, MEDICAL_INVOICE_HEADER_VERSION_NUMBER              as              MEDICAL_INVOICE_HEADER_VERSION_NUMBER
		, SOURCE_LINE_DLM_DATE_KEY                           as                           SOURCE_LINE_DLM_DATE_KEY
		, INVOICE_PROFILE_HKEY                               as                               INVOICE_PROFILE_HKEY
		, SUBMITTING_NETWORK_HKEY                            as                            SUBMITTING_NETWORK_HKEY
		, BWC_BILL_RECEIPT_DATE_KEY                          as                          BWC_BILL_RECEIPT_DATE_KEY
		, NETWORK_RECEIPT_DATE_KEY                           as                           NETWORK_RECEIPT_DATE_KEY
		, SERVICE_PROVIDER_HKEY                              as                              SERVICE_PROVIDER_HKEY
		, PAY_TO_PROVIDER_HKEY                               as                               PAY_TO_PROVIDER_HKEY
		, CPT_SERVICE_RENDERED_HKEY                          as                   HEALTHCARE_SERVICE_RENDERED_HKEY
		, MODIFIER_SEQUENCE_HKEY                             as                             MODIFIER_SEQUENCE_HKEY
		, INVOICE_SERVICE_FROM_DATE_KEY                      as                      INVOICE_SERVICE_FROM_DATE_KEY
		, INVOICE_SERVICE_TO_DATE_KEY                        as                        INVOICE_SERVICE_TO_DATE_KEY
		, LINE_SERVICE_FROM_DATE_KEY                         as                         LINE_SERVICE_FROM_DATE_KEY
		, LINE_SERVICE_TO_DATE_KEY                           as                           LINE_SERVICE_TO_DATE_KEY
		, REFERRING_PROVIDER_HKEY                            as                            REFERRING_PROVIDER_HKEY
		, PRINCIPAL_ICD_HKEY                                 as                                 PRINCIPAL_ICD_HKEY
		, ICD_1_HKEY                                         as                                         ICD_1_HKEY
		, ICD_2_HKEY                                         as                                         ICD_2_HKEY
		, ICD_3_HKEY                                         as                                         ICD_3_HKEY
		, ICD_4_HKEY                                         as                                         ICD_4_HKEY
		, INVOICE_LINE_ITEM_STATUS_HKEY                      as                      INVOICE_LINE_ITEM_STATUS_HKEY
		, INVOICE_HEADER_CURRENT_STATUS_HKEY                 as                 INVOICE_HEADER_CURRENT_STATUS_HKEY
		, FINANCIALLY_RESPONSIBLE_NETWORK_HKEY               as               FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
		, PAY_TO_NETWORK_HKEY                                as                                PAY_TO_NETWORK_HKEY
		, INTEREST_ACCRUAL_DATE_KEY                          as                          INTEREST_ACCRUAL_DATE_KEY
		, BWC_ADJUDICATION_DATE_KEY                          as                          BWC_ADJUDICATION_DATE_KEY
		, PAID_DATE_KEY                                      as                                      PAID_DATE_KEY
		, INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
		-- , CLAIM_NUMBER_HKEY                                  as                                  CLAIM_NUMBER_HKEY
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
		, ORIGINAL_WARRANT_DATE_KEY                          as                          ORIGINAL_WARRANT_DATE_KEY
		, ORIGINAL_WARRANT_NUMBER                            as                            ORIGINAL_WARRANT_NUMBER
		, INVOICE_SUBMITTED_POLICY_NUMBER                    as                    INVOICE_SUBMITTED_POLICY_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, LINE_UNITS_OF_BILLED_SERVICE_COUNT                 as                 LINE_UNITS_OF_BILLED_SERVICE_COUNT
		, LINE_UNITS_OF_PAID_SERVICE_COUNT                   as                   LINE_UNITS_OF_PAID_SERVICE_COUNT
		, LINE_PROVIDER_BILLED_AMOUNT                        as                        LINE_PROVIDER_BILLED_AMOUNT
		, LINE_MCO_ALLOWED_AMOUNT                            as                        LINE_NETWORK_ALLOWED_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT                as                LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
		, LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT            as            LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
		, LINE_BWC_APPROVED_AMOUNT                           as                           LINE_BWC_APPROVED_AMOUNT
		, LINE_INTEREST_AMOUNT                               as                               LINE_INTEREST_AMOUNT
		, LINE_REIMBURSED_AMOUNT                             as                             LINE_REIMBURSED_AMOUNT
		, INVOICE_BATCH_NUMBER                               as                              BUSINESS_BATCH_NUMBER 
				FROM     LOGIC_HIN   ), 
RENAME_NTWK as ( SELECT 
		  NETWORK_HKEY                                       as                                  NTWK_NETWORK_HKEY
		, CURRENT_ACQUIRING_MCO_NUMBER                       as                       CURRENT_ACQUIRING_MCO_NUMBER
		, NETWORK_NUMBER                                     as                                     NETWORK_NUMBER 
				FROM     LOGIC_NTWK   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_ASC                            as ( SELECT * from    RENAME_ASC   ),
FILTER_PBM                            as ( SELECT * from    RENAME_PBM   ),
FILTER_HOUT                           as ( SELECT * from    RENAME_HOUT   ),
FILTER_HIN                            as ( SELECT * from    RENAME_HIN   ),
FILTER_NTWK                           as ( SELECT * from    RENAME_NTWK   ),
FILTER_FNCL_NTWK                      as ( SELECT * from    RENAME_FNCL_NTWK 
                                            WHERE CURRENT_RECORD_IND = 'Y'  ),

----- UNION LAYER -----

UNION_CMB AS ( SELECT 
            MEDICAL_INVOICE_NUMBER
          , MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
          , MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
          , MEDICAL_INVOICE_LINE_VERSION_NUMBER
          , MEDICAL_INVOICE_HEADER_VERSION_NUMBER
          , SOURCE_LINE_DLM_DATE_KEY
          , INVOICE_PROFILE_HKEY
          , SUBMITTING_NETWORK_HKEY
          , BWC_BILL_RECEIPT_DATE_KEY
          , NETWORK_RECEIPT_DATE_KEY
          , SERVICE_PROVIDER_HKEY
          , PAY_TO_PROVIDER_HKEY
          , HEALTHCARE_SERVICE_RENDERED_HKEY
          , MODIFIER_SEQUENCE_HKEY
          , INVOICE_SERVICE_FROM_DATE_KEY
          , INVOICE_SERVICE_TO_DATE_KEY
          , LINE_SERVICE_FROM_DATE_KEY
          , LINE_SERVICE_TO_DATE_KEY
          , REFERRING_PROVIDER_HKEY
          , PRINCIPAL_ICD_HKEY
          , ICD_1_HKEY
          , ICD_2_HKEY
          , ICD_3_HKEY
          , ICD_4_HKEY
          , INVOICE_LINE_ITEM_STATUS_HKEY
          , INVOICE_HEADER_CURRENT_STATUS_HKEY
		 -- , FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
          , PAY_TO_NETWORK_HKEY
          , INTEREST_ACCRUAL_DATE_KEY
          , BWC_ADJUDICATION_DATE_KEY
          , PAID_DATE_KEY
          , INJURED_WORKER_HKEY
		  , CLAIM_TYPE_STATUS_HKEY
		  , CLAIM_DETAIL_HKEY
		  , POLICY_STANDING_HKEY
		  , ORIGINAL_WARRANT_DATE_KEY
		  , ORIGINAL_WARRANT_NUMBER
		  , CLAIM_NUMBER
		  , INVOICE_SUBMITTED_POLICY_NUMBER
          , LINE_UNITS_OF_BILLED_SERVICE_COUNT
          , LINE_UNITS_OF_PAID_SERVICE_COUNT
          , LINE_PROVIDER_BILLED_AMOUNT
          , LINE_NETWORK_ALLOWED_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
          , LINE_BWC_APPROVED_AMOUNT
          , LINE_INTEREST_AMOUNT
          , LINE_REIMBURSED_AMOUNT
          , BUSINESS_BATCH_NUMBER
		  , SUBMITTING_NETWORK_HKEY AS FNCL_JOIN_NTWK_COL
          			from  FILTER_ASC
          UNION ALL 
          SELECT 
		    MEDICAL_INVOICE_NUMBER
          , MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
          , MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
          , MEDICAL_INVOICE_LINE_VERSION_NUMBER
          , MEDICAL_INVOICE_HEADER_VERSION_NUMBER
          , SOURCE_LINE_DLM_DATE_KEY
          , INVOICE_PROFILE_HKEY
          , SUBMITTING_NETWORK_HKEY
          , BWC_BILL_RECEIPT_DATE_KEY
          , NETWORK_RECEIPT_DATE_KEY
          , SERVICE_PROVIDER_HKEY
          , PAY_TO_PROVIDER_HKEY
          , HEALTHCARE_SERVICE_RENDERED_HKEY
          , MODIFIER_SEQUENCE_HKEY
          , INVOICE_SERVICE_FROM_DATE_KEY
          , INVOICE_SERVICE_TO_DATE_KEY
          , LINE_SERVICE_FROM_DATE_KEY
          , LINE_SERVICE_TO_DATE_KEY
          , REFERRING_PROVIDER_HKEY
          , PRINCIPAL_ICD_HKEY
          , ICD_1_HKEY
          , ICD_2_HKEY
          , ICD_3_HKEY
          , ICD_4_HKEY
          , INVOICE_LINE_ITEM_STATUS_HKEY
          , INVOICE_HEADER_CURRENT_STATUS_HKEY
		 -- , FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
          , PAY_TO_NETWORK_HKEY
          , INTEREST_ACCRUAL_DATE_KEY
          , BWC_ADJUDICATION_DATE_KEY
          , PAID_DATE_KEY
          , INJURED_WORKER_HKEY
		  , CLAIM_TYPE_STATUS_HKEY
		  , CLAIM_DETAIL_HKEY
		  , POLICY_STANDING_HKEY
		  , ORIGINAL_WARRANT_DATE_KEY
		  , ORIGINAL_WARRANT_NUMBER
		  , INVOICE_SUBMITTED_POLICY_NUMBER
		  , CLAIM_NUMBER
          , LINE_UNITS_OF_BILLED_SERVICE_COUNT
          , LINE_UNITS_OF_PAID_SERVICE_COUNT
          , LINE_PROVIDER_BILLED_AMOUNT
          , LINE_NETWORK_ALLOWED_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
          , LINE_BWC_APPROVED_AMOUNT
          , LINE_INTEREST_AMOUNT
          , LINE_REIMBURSED_AMOUNT
          , BUSINESS_BATCH_NUMBER
		  , md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
)) AS FNCL_JOIN_NTWK_COL
          			from  FILTER_PBM
          UNION ALL 
          SELECT 
		    MEDICAL_INVOICE_NUMBER
          , MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
          , MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
          , MEDICAL_INVOICE_LINE_VERSION_NUMBER
          , MEDICAL_INVOICE_HEADER_VERSION_NUMBER
          , SOURCE_LINE_DLM_DATE_KEY
          , INVOICE_PROFILE_HKEY
          , SUBMITTING_NETWORK_HKEY
          , BWC_BILL_RECEIPT_DATE_KEY
          , NETWORK_RECEIPT_DATE_KEY
          , SERVICE_PROVIDER_HKEY
          , PAY_TO_PROVIDER_HKEY
          , HEALTHCARE_SERVICE_RENDERED_HKEY
          , MODIFIER_SEQUENCE_HKEY
          , INVOICE_SERVICE_FROM_DATE_KEY
          , INVOICE_SERVICE_TO_DATE_KEY
          , LINE_SERVICE_FROM_DATE_KEY
          , LINE_SERVICE_TO_DATE_KEY
          , REFERRING_PROVIDER_HKEY
          , PRINCIPAL_ICD_HKEY
          , ICD_1_HKEY
          , ICD_2_HKEY
          , ICD_3_HKEY
          , ICD_4_HKEY
          , INVOICE_LINE_ITEM_STATUS_HKEY
          , INVOICE_HEADER_CURRENT_STATUS_HKEY
		 -- , FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
          , PAY_TO_NETWORK_HKEY
          , INTEREST_ACCRUAL_DATE_KEY
          , BWC_ADJUDICATION_DATE_KEY
          , PAID_DATE_KEY
          , INJURED_WORKER_HKEY
		  , CLAIM_TYPE_STATUS_HKEY
		  , CLAIM_DETAIL_HKEY
		  , POLICY_STANDING_HKEY
		  , ORIGINAL_WARRANT_DATE_KEY
		  , ORIGINAL_WARRANT_NUMBER
		  , INVOICE_SUBMITTED_POLICY_NUMBER
		  , CLAIM_NUMBER
          , LINE_UNITS_OF_BILLED_SERVICE_COUNT
          , LINE_UNITS_OF_PAID_SERVICE_COUNT
          , LINE_PROVIDER_BILLED_AMOUNT
          , LINE_NETWORK_ALLOWED_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
          , LINE_BWC_APPROVED_AMOUNT
          , LINE_INTEREST_AMOUNT
          , LINE_REIMBURSED_AMOUNT
          , BUSINESS_BATCH_NUMBER
		  , SUBMITTING_NETWORK_HKEY AS FNCL_JOIN_NTWK_COL
          			from  FILTER_HOUT
          UNION ALL 
          SELECT 
		    MEDICAL_INVOICE_NUMBER
          , MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
          , MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
          , MEDICAL_INVOICE_LINE_VERSION_NUMBER
          , MEDICAL_INVOICE_HEADER_VERSION_NUMBER
          , SOURCE_LINE_DLM_DATE_KEY
          , INVOICE_PROFILE_HKEY
          , SUBMITTING_NETWORK_HKEY
          , BWC_BILL_RECEIPT_DATE_KEY
          , NETWORK_RECEIPT_DATE_KEY
          , SERVICE_PROVIDER_HKEY
          , PAY_TO_PROVIDER_HKEY
          , HEALTHCARE_SERVICE_RENDERED_HKEY
          , MODIFIER_SEQUENCE_HKEY
          , INVOICE_SERVICE_FROM_DATE_KEY
          , INVOICE_SERVICE_TO_DATE_KEY
          , LINE_SERVICE_FROM_DATE_KEY
          , LINE_SERVICE_TO_DATE_KEY
          , REFERRING_PROVIDER_HKEY
          , PRINCIPAL_ICD_HKEY
          , ICD_1_HKEY
          , ICD_2_HKEY
          , ICD_3_HKEY
          , ICD_4_HKEY
          , INVOICE_LINE_ITEM_STATUS_HKEY
          , INVOICE_HEADER_CURRENT_STATUS_HKEY
		 -- , FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
          , PAY_TO_NETWORK_HKEY
          , INTEREST_ACCRUAL_DATE_KEY
          , BWC_ADJUDICATION_DATE_KEY
          , PAID_DATE_KEY
          , INJURED_WORKER_HKEY
		  , CLAIM_TYPE_STATUS_HKEY    
		  , CLAIM_DETAIL_HKEY
		  , POLICY_STANDING_HKEY
		  , ORIGINAL_WARRANT_DATE_KEY
		  , ORIGINAL_WARRANT_NUMBER
		  , INVOICE_SUBMITTED_POLICY_NUMBER
		  , CLAIM_NUMBER
          , LINE_UNITS_OF_BILLED_SERVICE_COUNT
          , LINE_UNITS_OF_PAID_SERVICE_COUNT
          , LINE_PROVIDER_BILLED_AMOUNT
          , LINE_NETWORK_ALLOWED_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
          , LINE_BWC_APPROVED_AMOUNT
          , LINE_INTEREST_AMOUNT
          , LINE_REIMBURSED_AMOUNT
          , BUSINESS_BATCH_NUMBER
		  , SUBMITTING_NETWORK_HKEY AS FNCL_JOIN_NTWK_COL
          			from  FILTER_HIN
),


---- JOIN LAYER ----
JOIN_ETL as ( SELECT UNION_CMB.*,  FILTER_FNCL_NTWK.FINANCIALLY_RESPONSIBLE_NETWORK_HKEY 
				FROM  UNION_CMB
				LEFT JOIN FILTER_NTWK ON  UNION_CMB.FNCL_JOIN_NTWK_COL =  FILTER_NTWK.NTWK_NETWORK_HKEY
				LEFT JOIN FILTER_FNCL_NTWK ON COALESCE(FILTER_NTWK.CURRENT_ACQUIRING_MCO_NUMBER, FILTER_NTWK.NETWORK_NUMBER) =  FILTER_FNCL_NTWK.FNCL_NTWK_NETWORK_NUMBER 
						  ),

---- ETL LAYER ----
ETL AS ( SELECT 
            MEDICAL_INVOICE_NUMBER
          , MEDICAL_INVOICE_LINE_SEQUENCE_NUMBER
          , MEDICAL_INVOICE_LINE_EXTENSION_NUMBER
          , MEDICAL_INVOICE_LINE_VERSION_NUMBER
          , MEDICAL_INVOICE_HEADER_VERSION_NUMBER
          , SOURCE_LINE_DLM_DATE_KEY
          , INVOICE_PROFILE_HKEY
          , SUBMITTING_NETWORK_HKEY
          , BWC_BILL_RECEIPT_DATE_KEY
          , NETWORK_RECEIPT_DATE_KEY
          , SERVICE_PROVIDER_HKEY
          , PAY_TO_PROVIDER_HKEY
          , HEALTHCARE_SERVICE_RENDERED_HKEY
          , MODIFIER_SEQUENCE_HKEY
          , INVOICE_SERVICE_FROM_DATE_KEY
          , INVOICE_SERVICE_TO_DATE_KEY
          , LINE_SERVICE_FROM_DATE_KEY
          , LINE_SERVICE_TO_DATE_KEY
          , REFERRING_PROVIDER_HKEY
          , PRINCIPAL_ICD_HKEY
          , ICD_1_HKEY
          , ICD_2_HKEY
          , ICD_3_HKEY
          , ICD_4_HKEY
          , INVOICE_LINE_ITEM_STATUS_HKEY
          , INVOICE_HEADER_CURRENT_STATUS_HKEY
          , FINANCIALLY_RESPONSIBLE_NETWORK_HKEY
          , PAY_TO_NETWORK_HKEY
          , INTEREST_ACCRUAL_DATE_KEY
          , BWC_ADJUDICATION_DATE_KEY
          , PAID_DATE_KEY
          , INJURED_WORKER_HKEY
		  , CLAIM_TYPE_STATUS_HKEY
		  , CLAIM_DETAIL_HKEY
		  , POLICY_STANDING_HKEY
		  , ORIGINAL_WARRANT_DATE_KEY
		  , ORIGINAL_WARRANT_NUMBER
		  , CLAIM_NUMBER
		  , INVOICE_SUBMITTED_POLICY_NUMBER
          , LINE_UNITS_OF_BILLED_SERVICE_COUNT
          , LINE_UNITS_OF_PAID_SERVICE_COUNT
          , LINE_PROVIDER_BILLED_AMOUNT
          , LINE_NETWORK_ALLOWED_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_LOOKUP_AMOUNT
          , LINE_BWC_FEE_SCHEDULE_CALCULATED_AMOUNT
          , LINE_BWC_APPROVED_AMOUNT
          , LINE_INTEREST_AMOUNT
          , LINE_REIMBURSED_AMOUNT
          , BUSINESS_BATCH_NUMBER		  
		  , CURRENT_TIMESTAMP AS LOAD_DATETIME
		  , TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
		  , 'CAM' AS PRIMARY_SOURCE_SYSTEM
          from JOIN_ETL
)

SELECT * FROM ETL