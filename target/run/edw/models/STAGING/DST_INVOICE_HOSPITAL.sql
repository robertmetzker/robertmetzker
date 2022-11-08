

      create or replace  table DEV_EDW.STAGING.DST_INVOICE_HOSPITAL  as
      (---- SRC LAYER ----
WITH
SRC_IH as ( SELECT *     from     STAGING.STG_INVOICE_HEADER ),
SRC_PART as ( SELECT *     from     STAGING.STG_PARTICIPATION ),
SRC_IL as ( SELECT *     from     STAGING.STG_INVOICE_LINE ),
SRC_NTWK as ( SELECT *     from     STAGING.STG_NETWORK ),
SRC_PROVT as ( SELECT *     from     STAGING.STG_PROVIDER_TYPE ),
SRC_PYC as ( SELECT *     from     STAGING.STG_CPT_PAYMENT_CATEGORY ),
SRC_VR as ( SELECT *     from     STAGING.STG_CPT_VR_FEE_SCHEDULE ),
SRC_DEP as ( SELECT *     from     STAGING.STG_CPT_DEP_FEE_SCHEDULE ),
SRC_CL as ( SELECT *     from     STAGING.DST_CLAIM ),
SRC_APC as ( SELECT *     from     STAGING.STG_INVOICE_LINE_APC ),
SRC_DRG as ( SELECT *     from     STAGING.STG_INVOICE_DRG ),
SRC_ID as ( SELECT *     from     STAGING.STG_INVOICE_DIAGNOSIS ),
SRC_CSH as ( SELECT *     from     STAGING.DST_CLAIM_STATUS_HISTORY ),
SRC_DPC as ( SELECT *     from     STAGING.STG_DETAIL_PAYMENT_CODING ),
SRC_OPPS as ( SELECT *     from     STAGING.STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION ),
SRC_IPPS as ( SELECT *     from     STAGING.STG_INPATIENT_PROVIDER_SPECIFIC_INFORMATION ),
SRC_CTH as ( SELECT *     from     STAGING.DST_CLAIM_TYPE_HISTORY ),
SRC_PSR as ( SELECT *     from     STAGING.STG_POLICY_STATUS_REASON_HISTORY ),
//SRC_IH as ( SELECT *     from     STG_INVOICE_HEADER) ,
//SRC_PART as ( SELECT *     from     STG_PARTICIPATION) ,
//SRC_IL as ( SELECT *     from     STG_INVOICE_LINE) ,
//SRC_NTWK as ( SELECT *     from     STG_NETWORK) ,
//SRC_PROVT as ( SELECT *     from     STG_PROVIDER_TYPE) ,
//SRC_PYC as ( SELECT *     from     STG_CPT_PAYMENT_CATEGORY) ,
//SRC_VR as ( SELECT *     from     STG_CPT_VR_FEE_SCHEDULE) ,
//SRC_DEP as ( SELECT *     from     STG_CPT_DEP_FEE_SCHEDULE) ,
//SRC_CL as ( SELECT *     from     DST_CLAIM) ,
//SRC_APC as ( SELECT *     from     STG_INVOICE_LINE_APC) ,
//SRC_DRG as ( SELECT *     from     STG_INVOICE_DRG) ,
//SRC_ID as ( SELECT *     from     STG_INVOICE_DIAGNOSIS) ,
//SRC_CSH as ( SELECT *     from     DST_CLAIM_STATUS_HISTORY) ,
//SRC_DPC as ( SELECT *     from     STG_DETAIL_PAYMENT_CODING) ,
//SRC_OPPS as ( SELECT *     from     STG_OUTPATIENT_PROVIDER_SPECIFIC_INFORMATION) ,
//SRC_IPPS as ( SELECT *     from     STG_INPATIENT_PROVIDER_SPECIFIC_INFORMATION) ,
//SRC_CTH as ( SELECT *     from     DST_CLAIM_TYPE_HISTORY) ,
//SRC_PSR as ( SELECT *     from     STG_POLICY_STATUS_REASON_HISTORY) ,

---- LOGIC LAYER ----

LOGIC_IH as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, TRIM( INVOICE_NUMBER )                             as                                     INVOICE_NUMBER 
		, TRIM( MCO_NUMBER )                                 as                                         MCO_NUMBER 
		, TRIM( CLAIM_NUMBER )                               as                                       CLAIM_NUMBER 
		, TRIM( POLICY_NUMBER )                              as                                      POLICY_NUMBER 
		, SRVCN_PRO_ID                                       as                                       SRVCN_PRO_ID 
		, TRIM( SRVCN_PEACH_NUMBER )                         as                                 SRVCN_PEACH_NUMBER 
		, PAYTO_PRO_ID                                       as                                       PAYTO_PRO_ID 
		, TRIM( PAYTO_PEACH_NUMBER )                         as                                 PAYTO_PEACH_NUMBER 
		, OTHER_PHYSICIAN1                                   as                                   OTHER_PHYSICIAN1 
		, TRIM( PRESCRIBING_PHYISCIAN )                      as                              PRESCRIBING_PHYISCIAN 
		, TRIM( INVOICE_TYPE )                               as                                       INVOICE_TYPE 
		, upper( TRIM( INVOICE_TYPE_DESC ) )                 as                                  INVOICE_TYPE_DESC 
		, TRIM( INVOICE_STATUS )                             as                                     INVOICE_STATUS 
		, TRIM( INVOICE_STATUS_DESC )                        as                                INVOICE_STATUS_DESC 
		, ENTRY_DATE                                         as                                         ENTRY_DATE 
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE 
		, NTWK_RECEIPT_DATE                                  as                                  NTWK_RECEIPT_DATE 
		, SERVICE_FROM                                       as                                       SERVICE_FROM 
		, SERVICE_TO                                         as                                         SERVICE_TO 
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER 
		, BATCH_SEQUENCE                                     as                                     BATCH_SEQUENCE 
		, TRIM( BATCH_TYPE )                                 as                                         BATCH_TYPE 
		, EXTENSION_NUMBER                                   as                                   EXTENSION_NUMBER 
		, VERSION_NUMBER                                     as                                     VERSION_NUMBER 
		, PAYEE_ID                                           as                                           PAYEE_ID 
		, TRIM( NTWK_PAYEE_IND )                             as                                     NTWK_PAYEE_IND 
		, TRIM( PAYEE_TYPE )                                 as                                         PAYEE_TYPE 
		, ADMISSION_DATE                                     as                                     ADMISSION_DATE 
		, TRIM( ADMISSION_HOURS )                            as                                    ADMISSION_HOURS 
		, TRIM( ADMISSION_TYPE )                             as                                     ADMISSION_TYPE 
		, TRIM( ADMISSION_SOURCE )                           as                                   ADMISSION_SOURCE 
		, TRIM( DISCHARGE_STATUS )                           as                                   DISCHARGE_STATUS 
		, DISCHARGE_DATE                                     as                                     DISCHARGE_DATE 
		, TRIM( DISCHARGE_HOURS )                            as                                    DISCHARGE_HOURS 
		, BILL_DATE                                          as                                          BILL_DATE 
		, TRIM( BILL_TYPE )                                  as                                          BILL_TYPE 
		, TRIM( PRO_DRG )                                    as                                            PRO_DRG 
		, CASE WHEN LENGTH(TRIM(PRO_DRG)) = 4  AND LEFT(TRIM(PRO_DRG),1)='0' 
        		THEN RIGHT(TRIM(PRO_DRG),3) 
				WHEN LENGTH(TRIM(PRO_DRG)) < 3 
        		THEN LPAD(TRIM(PRO_DRG),3,'0') ELSE  TRIM(PRO_DRG) END		AS 						FRMTTD_PRO_DRG
		, TRIM( PATIENT_ACCT_NUMBER )                        AS                                PATIENT_ACCT_NUMBER
		, REFERRING_PRO_ID                                   as                                   REFERRING_PRO_ID 
		, TRIM( REFERRING_PEACH_NUMBER )                     as                             REFERRING_PEACH_NUMBER 
		, TRIM( ULM )                                        as                                                ULM 
		, DLM                                                as                                                DLM 
		, TRIM( MOD_SET )                                    as                                            MOD_SET 
		, TRIM( DIAGNOSIS_CODE )                             as                                     DIAGNOSIS_CODE
		, case when MCO_NUMBER = '20001' then 'S'
            when BATCH_TYPE='M' then 'M' 
            when BATCH_TYPE='E' then 'EDI'
            else NULL END 									 as 								 INPUT_METHOD_CODE 
		, TOTAL_APPROVED                                     as                                     TOTAL_APPROVED 
		, CASE WHEN INVOICE_STATUS_DESC = 'PAID' 
            	AND TOTAL_APPROVED > 0 THEN 'Y' ELSE 'N' END as 							   PAID_ABOVE_ZERO_IND 
		, 'SUBROGATION_TYPE_DESC'                      		 as                              SUBROGATION_TYPE_DESC 
 		from SRC_IH
            ),
LOGIC_PART as ( SELECT 
		  TRIM( CUST_NO )                                    as                                            CUST_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, TRIM( CP_VOID_IND )                                as                                        CP_VOID_IND 
		from SRC_PART
            ),
LOGIC_IL as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, INVOICE_LINE_ID                                    as                                    INVOICE_LINE_ID 
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE 
		, SEQUENCE_EXTENSION                                 as                                 SEQUENCE_EXTENSION 
		, VERSION_NUMBER                                     as                                     VERSION_NUMBER 
		, PAID_MCO                                           as                                           PAID_MCO 
		, INT_DATE                                           as                                           INT_DATE 
		, DATE_OF_SVC_FROM                                   as                                   DATE_OF_SVC_FROM 
		, DATE_OF_SVC_TO                                     as                                     DATE_OF_SVC_TO 
		, TRIM( SERVICE_CODE1 )                              as                                      SERVICE_CODE1 
		, TRIM( SERVICE_CODE2 )                              as                                      SERVICE_CODE2 
		, case when length(trim(SERVICE_CODE1))=5 then trim(SERVICE_CODE1)
            when length(trim(SERVICE_CODE1))=4 and length(trim(SERVICE_CODE2))=5 then NULLIF(trim(SERVICE_CODE2),'')
            else NULL END 									 as 									PROCEDURE_CODE
        , case when length(SERVICE_CODE1)=4 then lpad(trim(SERVICE_CODE1),4,'0')
            else NULL END 									 as 							   REVENUE_CENTER_CODE
        , case when length(SERVICE_CODE1)=11 then trim(SERVICE_CODE1) 
            else NULL END 									 as								    NATIONAL_DRUG_CODE                                           
		, upper( TRIM( PLACE_OF_SVC_CODE ) )                 as                                  PLACE_OF_SVC_CODE 
		, TRIM( PLACE_OF_SVC_DESC )                          as                                  PLACE_OF_SVC_DESC 
		, TRIM( LINE_STATUS_CODE )                           as                                   LINE_STATUS_CODE 
		, TRIM( LINE_STATUS_DESC )                           as                                   LINE_STATUS_DESC 
		, LINE_STATUS_DATE                                   as                                   LINE_STATUS_DATE 
		, TRIM( ADJUDICATION_STATUS_CODE )                   as                           ADJUDICATION_STATUS_CODE 
		, TRIM( ADJUDICATION_STATUS_DESC )                   as                           ADJUDICATION_STATUS_DESC 
		, ADJUDICATION_STATUS_DATE                           as                           ADJUDICATION_STATUS_DATE 
		, TRIM( REVERSAL_IND )                               as                                       REVERSAL_IND 
		, TRIM( OVERRIDE_IND )                               as                                       OVERRIDE_IND 
		, TRIM( DENIED_FLAG )                                as                                        DENIED_FLAG 
		, TRIM( PAYMENT_NUMBER )                             as                                     PAYMENT_NUMBER 
		, PAYMENT_DATE                                       as                                       PAYMENT_DATE 
		, TRIM( DIAGNOSIS1 )                                 as                                         DIAGNOSIS1 
		, TRIM( DIAGNOSIS2 )                                 as                                         DIAGNOSIS2 
		, TRIM( DIAGNOSIS3 )                                 as                                         DIAGNOSIS3 
		, TRIM( DIAGNOSIS4 )                                 as                                         DIAGNOSIS4 
		, EDI_ID                                             as                                             EDI_ID 
		, ENTRY_DATE                                         as                                         ENTRY_DATE 
		, TRIM( ENTRY_USER )                                 as                                         ENTRY_USER 
		, DLM                                                as                                                DLM 
		, TRIM( ULM )                                        as                                                ULM 
		, PAID_UNITS                                         as                                         PAID_UNITS 
		, UNITS_OF_SERVICE                                   as                                   UNITS_OF_SERVICE 
		, BILLED_AMOUNT                                      as                                      BILLED_AMOUNT 
		, CALC_AMOUNT                                        as                                        CALC_AMOUNT 
		, NON_COVERED_AMOUNT                                 as                                 NON_COVERED_AMOUNT 
		, APPROVED_AMOUNT                                    as                                    APPROVED_AMOUNT 
		, NTWK_BILLED_AMT                                    as                                    NTWK_BILLED_AMT 
		, FEE_SCHED_AMOUNT                                   as                                   FEE_SCHED_AMOUNT 
		, TRIM( INTEREST )                                   as                                           INTEREST 
		, PMT_AMT                                            as                                            PMT_AMT
		, TRIM( MOD1_MODIFIER_CODE )                         as                                 MOD1_MODIFIER_CODE 
		, TRIM( MOD2_MODIFIER_CODE )                         as                                 MOD2_MODIFIER_CODE 
		, TRIM( MOD3_MODIFIER_CODE )                         as                                 MOD3_MODIFIER_CODE 
		, TRIM( MOD4_MODIFIER_CODE )                         as                                 MOD4_MODIFIER_CODE 
		from SRC_IL
            ),
LOGIC_NTWK as ( SELECT 
		  LPAD(TRIM(NTWK_NUMBER),5,'0')                      as                                        NTWK_NUMBER                                          
		, TRIM( NTWK_ID )                                    as                                            NTWK_ID 
		, END_DATE                                           as                                           END_DATE 
		from SRC_NTWK
            ),
LOGIC_PROVT as ( SELECT 
		  case when PRVDR_TYPE_CODE = '60' then 'Y' else 'N' END  as							  SUBROGATION_FLAG
		, PRVDR_BASE_NMBR::varchar||lpad(PRVDR_SFX_NMBR::varchar,4,'0') as                      PROVT_PEACH_NUMBER
		, TRIM( PRVDR_BASE_NMBR )                            as                                    PRVDR_BASE_NMBR 
		, TRIM( PRVDR_SFX_NMBR )                             as                                     PRVDR_SFX_NMBR 
		, TRIM( PRVDR_TYPE_CODE )                            as                                    PRVDR_TYPE_CODE 
		, DCTVT_DTTM                                         as                                         DCTVT_DTTM 
		from SRC_PROVT
            ),
LOGIC_PYC as ( SELECT 
		  TRIM( PAYMENT_CATEGORY )                           AS                                   PAYMENT_CATEGORY 
		, TRIM( PROCEDURE_CODE )                             as                                     PROCEDURE_CODE 
		from SRC_PYC
            ),
LOGIC_VR as ( SELECT 
		  TRIM( PROCEDURE_CODE )                             as                                     PROCEDURE_CODE 
		, TRIM( FEE_SCHEDULE )                               as                                       FEE_SCHEDULE 
		from SRC_VR
            ),
LOGIC_DEP as ( SELECT 
		  TRIM( PROCEDURE_CODE )                             as                                     PROCEDURE_CODE 
		, TRIM( FEE_SCHEDULE )                               as                                       FEE_SCHEDULE 
		from SRC_DEP
            ),
LOGIC_CL as ( SELECT 
		  TRIM( CLM_REL_SNPSHT_IND )                         as                                 CLM_REL_SNPSHT_IND 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, AGRE_ID                                            as                                            AGRE_ID 
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
		, TRIM( K_PROGRAM_REASON_DESC )                      as                              K_PROGRAM_REASON_DESC 

		from SRC_CL
            ),
LOGIC_APC as ( SELECT 
		  INVOICE_LINE_ID                                    as                                    INVOICE_LINE_ID 
		, TRIM( STATUS )                                     as                                             STATUS 
		, TRIM( APC_CODE )                                   as                                           APC_CODE 
		, BASE_AMOUNT                                        as                                        BASE_AMOUNT 
		, OUTLIER                                            as                                            OUTLIER 
		, RURAL                                              as                                              RURAL 
		, HOLD_HARMLESS                                      as                                      HOLD_HARMLESS 
		, MEDICARE_AMOUNT                                    as                                    MEDICARE_AMOUNT 
		, MARKUP                                             as                                             MARKUP 
		, CALC_TOTAL                                         as                                         CALC_TOTAL 
		, TRIM( DISCOUNT )                                   as                                           DISCOUNT 
		, FINAL_MARKUP                                       as                                       FINAL_MARKUP 
		, TRIM( COMPOSITE_ADJUSTMENT )                       as                               COMPOSITE_ADJUSTMENT 
		, TRIM( NON_OPPS_BILL_TYPE )                         as                                 NON_OPPS_BILL_TYPE 
		, TRIM( OPPS_FLAG )                                  as                                          OPPS_FLAG 
		, TRIM( PAYMENT_METHOD )                             as                                     PAYMENT_METHOD 
		, SERVICE_UNITS                                      as                                      SERVICE_UNITS 
		, TRIM( PACKAGING )                                  as                                          PACKAGING 
		, TRIM( PAYMENT_ADJUSTMENT )                         as                                 PAYMENT_ADJUSTMENT 
		, TRIM( PAYMENT_INDICATOR )                          as                                  PAYMENT_INDICATOR 
		from SRC_APC
            ),
LOGIC_DRG as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, TRIM( DRG_NUMBER )                                 as                                         DRG_NUMBER 
		from SRC_DRG
            ),
LOGIC_ID as ( SELECT 
		  INVOICE_HEADER_ID                                  as                                  INVOICE_HEADER_ID 
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER 
		, TRIM( DIAGNOSIS_CODE )                             as                                     DIAGNOSIS_CODE 
		from SRC_ID
            ),
LOGIC_CSH as ( SELECT 
		  TRIM( CLM_STT_TYP_CD )                             as                                     CLM_STT_TYP_CD 
		, TRIM( CLM_STS_TYP_CD )                             as                                     CLM_STS_TYP_CD 
		, TRIM( CLM_TRANS_RSN_TYP_CD )                       as                               CLM_TRANS_RSN_TYP_CD 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		from SRC_CSH
            ),
LOGIC_DPC as ( SELECT 
		  TRIM( TCN_NO )                                     AS                                             TCN_NO 
		, WRNT_DATE                                          as                                          WRNT_DATE 
		, TRIM( WRNT_NO )                                    as                                            WRNT_NO 
		from SRC_DPC
            ),
LOGIC_OPPS as ( SELECT 
		  EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE 
		, TRIM( PRO_NUM )                                    as                                            PRO_NUM 
		, PAY_TO_COST                                        as                                        PAY_TO_COST 
		, OUT_CCR                                            as                                            OUT_CCR 
		, DEV_CCR                                            as                                            DEV_CCR 
		, PUB_DEV_CCR                                        as                                        PUB_DEV_CCR 
		, WAGE_INDEX                                         as                                         WAGE_INDEX 
		, TRIM( PROVIDER_CBSA_CODE )                         as                                 PROVIDER_CBSA_CODE 
		from SRC_OPPS
            ),
LOGIC_IPPS as ( SELECT 
		  TRIM( PRO_NUM )                                    as                                            PRO_NUM 
		, EFFECTIVE_DATE                                     as                                     EFFECTIVE_DATE 
		, EXPIRATION_DATE                                    as                                    EXPIRATION_DATE 
		, IN_CCR                                             as                                             IN_CCR 
		, DGME                                               as                                               DGME 
		from SRC_IPPS
            ),
LOGIC_CTH as ( SELECT 
		  TRIM( CLM_NO )                                     as                                             CLM_NO 
		, TRIM( CLM_TYP_CD )                                 as                                         CLM_TYP_CD 
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT 
		, HIST_END_DT                                        as                                        HIST_END_DT 
		, TRIM( CHNG_OVR_IND )                               as                                       CHNG_OVR_IND 
		from SRC_CTH
		    ),

LOGIC_PSR as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, TRIM( MOST_RCNT_PLCY_STS_RSN_IND )                 as                         MOST_RCNT_PLCY_STS_RSN_IND 
		, TRIM( CRNT_PLCY_PRD_STS_RSN_IND )                  as                          CRNT_PLCY_PRD_STS_RSN_IND 
		, TRIM( PLCY_TYP_CODE )                              as                                      PLCY_TYP_CODE 
		, TRIM( PLCY_STS_TYP_CD )                            as                                    PLCY_STS_TYP_CD 
		, TRIM( PLCY_STS_RSN_TYP_CD )                        as                                PLCY_STS_RSN_TYP_CD
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE 
		, case when PLCY_STS_TYP_CD IN ('EXP', 'ACT') then 'Y'
		       else 'N' end                                  as                                  POLICY_ACTIVE_IND 
		from SRC_PSR
            )		

---- RENAME LAYER ----
,

RENAME_IH as ( SELECT 
		  INVOICE_HEADER_ID                                  as                           HEADER_INVOICE_HEADER_ID
		, INVOICE_NUMBER                                     as                                     INVOICE_NUMBER
		, MCO_NUMBER                                         as                                         MCO_NUMBER
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, POLICY_NUMBER                                      as                                      POLICY_NUMBER
		, SRVCN_PRO_ID                                       as                              SERVICING_PROVIDER_ID
		, SRVCN_PEACH_NUMBER                                 as                             SERVICING_PEACH_NUMBER
		, PAYTO_PRO_ID                                       as                                  PAYTO_PROVIDER_ID
		, PAYTO_PEACH_NUMBER                                 as                                 PAYTO_PEACH_NUMBER
		, OTHER_PHYSICIAN1                                   as                                   OTHER_PHYSICIAN1
		, PRESCRIBING_PHYISCIAN                              as                              PRESCRIBING_PHYISCIAN
		, INVOICE_TYPE                                       as                                       INVOICE_TYPE
		, INVOICE_TYPE_DESC                                  as                                  INVOICE_TYPE_DESC
		, INVOICE_STATUS                                     as                                     INVOICE_STATUS
		, INVOICE_STATUS_DESC                                as                                INVOICE_STATUS_DESC
		, ENTRY_DATE                                         as                                  HEADER_ENTRY_DATE
		, RECEIPT_DATE                                       as                                       RECEIPT_DATE
		, NTWK_RECEIPT_DATE                                  as                               NETWORK_RECEIPT_DATE
		, SERVICE_FROM                                       as                                       SERVICE_FROM
		, SERVICE_TO                                         as                                         SERVICE_TO
		, BATCH_NUMBER                                       as                                       BATCH_NUMBER
		, BATCH_SEQUENCE                                     as                                     BATCH_SEQUENCE
		, BATCH_TYPE                                         as                                         BATCH_TYPE
		, EXTENSION_NUMBER                                   as                                   EXTENSION_NUMBER
		, VERSION_NUMBER                                     as                              HEADER_VERSION_NUMBER
		, PAYEE_ID                                           as                                           PAYEE_ID
		, NTWK_PAYEE_IND                                     as                                  NETWORK_PAYEE_IND
		, PAYEE_TYPE                                         as                                         PAYEE_TYPE
		, ADMISSION_DATE                                     as                                     ADMISSION_DATE
		, ADMISSION_HOURS                                    as                                    ADMISSION_HOURS
		, ADMISSION_TYPE                                     as                                     ADMISSION_TYPE
		, ADMISSION_SOURCE                                   as                                   ADMISSION_SOURCE
		, DISCHARGE_STATUS                                   as                                   DISCHARGE_STATUS
		, DISCHARGE_DATE                                     as                                     DISCHARGE_DATE
		, DISCHARGE_HOURS                                    as                                    DISCHARGE_HOURS
		, BILL_DATE                                          as                                          BILL_DATE
		, BILL_TYPE                                          as                                          BILL_TYPE
		, PRO_DRG                                            as                                            PRO_DRG
		, FRMTTD_PRO_DRG                                     as                                     FRMTTD_PRO_DRG
		, PATIENT_ACCT_NUMBER                                as                             PATIENT_ACCOUNT_NUMBER
		, REFERRING_PRO_ID                                   as                              REFERRING_PROVIDER_ID
		, REFERRING_PEACH_NUMBER                             as                             REFERRING_PEACH_NUMBER
		, ULM                                                as                                         HEADER_ULM
		, DLM                                                as                                         HEADER_DLM
		, MOD_SET                                            as                                            MOD_SET
		, DIAGNOSIS_CODE                                     as                                     DIAGNOSIS_CODE
		, INPUT_METHOD_CODE                                  as                                  INPUT_METHOD_CODE
		, PAID_ABOVE_ZERO_IND                                as                                PAID_ABOVE_ZERO_IND
		, SUBROGATION_TYPE_DESC                              as                              SUBROGATION_TYPE_DESC
		, TOTAL_APPROVED                                     as                              TOTAL_APPROVED_AMOUNT 
				FROM     LOGIC_IH   ), 
RENAME_PART as ( SELECT 
		  CUST_NO                                            as                                            CUST_NO
		, AGRE_ID                                            as                                       PART_AGRE_ID
		, PTCP_TYP_CD                                        as                                   PART_PTCP_TYP_CD
		, CP_VOID_IND                                        as                                   PART_CP_VOID_IND 
				FROM     LOGIC_PART   ), 
RENAME_IL as ( SELECT 
		  INVOICE_HEADER_ID                                  as                             LINE_INVOICE_HEADER_ID
		, INVOICE_LINE_ID                                    as                                    INVOICE_LINE_ID
		, LINE_SEQUENCE                                      as                                      LINE_SEQUENCE
		, SEQUENCE_EXTENSION                                 as                                 SEQUENCE_EXTENSION
		, VERSION_NUMBER                                     as                                LINE_VERSION_NUMBER
		, PAID_MCO                                           as                                        PAID_MCO_ID
		, INT_DATE                                           as                              INTEREST_ACCRUAL_DATE
		, DATE_OF_SVC_FROM                                   as                               DATE_OF_SERVICE_FROM
		, DATE_OF_SVC_TO                                     as                                 DATE_OF_SERVICE_TO
		, SERVICE_CODE1                                      as                                      SERVICE_CODE1
		, SERVICE_CODE2                                      as                                      SERVICE_CODE2
		, PROCEDURE_CODE                                     as                                     PROCEDURE_CODE
		, REVENUE_CENTER_CODE                                as                                REVENUE_CENTER_CODE
		, NATIONAL_DRUG_CODE                                 as                                 NATIONAL_DRUG_CODE
		, PLACE_OF_SVC_CODE                                  as                              PLACE_OF_SERVICE_CODE
		, PLACE_OF_SVC_DESC                                  as                              PLACE_OF_SERVICE_DESC
		, LINE_STATUS_CODE                                   as                                   LINE_STATUS_CODE
		, LINE_STATUS_DESC                                   as                                   LINE_STATUS_DESC
		, LINE_STATUS_DATE                                   as                                   LINE_STATUS_DATE
		, ADJUDICATION_STATUS_CODE                           as                           ADJUDICATION_STATUS_CODE
		, ADJUDICATION_STATUS_DESC                           as                           ADJUDICATION_STATUS_DESC
		, ADJUDICATION_STATUS_DATE                           as                           ADJUDICATION_STATUS_DATE
		, REVERSAL_IND                                       as                                       REVERSAL_IND
		, OVERRIDE_IND                                       as                                       OVERRIDE_IND
		, DENIED_FLAG                                        as                                        DENIED_FLAG
		, PAYMENT_NUMBER                                     as                                     PAYMENT_NUMBER
		, PAYMENT_DATE                                       as                                       PAYMENT_DATE
		, DIAGNOSIS1                                         as                                         DIAGNOSIS1
		, DIAGNOSIS2                                         as                                         DIAGNOSIS2
		, DIAGNOSIS3                                         as                                         DIAGNOSIS3
		, DIAGNOSIS4                                         as                                         DIAGNOSIS4
		, EDI_ID                                             as                                             EDI_ID
		, ENTRY_DATE                                         as                                    LINE_ENTRY_DATE
		, ENTRY_USER                                         as                                         ENTRY_USER
		, DLM                                                as                                           LINE_DLM
		, ULM                                                as                                           LINE_ULM
		, PAID_UNITS                                         as                                         PAID_UNITS
		, UNITS_OF_SERVICE                                   as                                   UNITS_OF_SERVICE
		, BILLED_AMOUNT                                      as                                      BILLED_AMOUNT
		, CALC_AMOUNT                                        as                                        CALC_AMOUNT
		, NON_COVERED_AMOUNT                                 as                                 NON_COVERED_AMOUNT
		, APPROVED_AMOUNT                                    as                                    APPROVED_AMOUNT
		, NTWK_BILLED_AMT                                    as                                    NTWK_BILLED_AMT
		, FEE_SCHED_AMOUNT                                   as                                   FEE_SCHED_AMOUNT
		, INTEREST                                           as                                           INTEREST
		, PMT_AMT                                            as                                            PMT_AMT
		, MOD1_MODIFIER_CODE                                 as                                 MOD1_MODIFIER_CODE
		, MOD2_MODIFIER_CODE                                 as                                 MOD2_MODIFIER_CODE
		, MOD3_MODIFIER_CODE                                 as                                 MOD3_MODIFIER_CODE
		, MOD4_MODIFIER_CODE                                 as                                 MOD4_MODIFIER_CODE 
				FROM     LOGIC_IL   ), 
RENAME_NTWK as ( SELECT 
		  NTWK_NUMBER                                        as                                    PAID_MCO_NUMBER
		, NTWK_ID                                            as                                            NTWK_ID
		, END_DATE                                           as                                           END_DATE 
				FROM     LOGIC_NTWK   ), 
RENAME_PROVT as ( SELECT 
		  SUBROGATION_FLAG                                   as                                   SUBROGATION_FLAG
		, PROVT_PEACH_NUMBER                                 as                                 PROVT_PEACH_NUMBER
		, PRVDR_BASE_NMBR                                    as                                  PEACH_BASE_NUMBER
		, PRVDR_SFX_NMBR                                     as                                PEACH_SUFFIX_NUMBER
		, PRVDR_TYPE_CODE                                    as                              PAYTO_PRVDR_TYPE_CODE
		, DCTVT_DTTM                                         as                                   PROVT_DCTVT_DTTM 
				FROM     LOGIC_PROVT   ), 
RENAME_PYC as ( SELECT 
		  PAYMENT_CATEGORY                                   as                                   PAYMENT_CATEGORY
		, PROCEDURE_CODE                                     as                                 PYC_PROCEDURE_CODE 
				FROM     LOGIC_PYC   ), 
RENAME_VR as ( SELECT 
		  PROCEDURE_CODE                                     as                                  VR_PROCEDURE_CODE
		, FEE_SCHEDULE                                       as                                    VR_FEE_SCHEDULE 
				FROM     LOGIC_VR   ), 
RENAME_DEP as ( SELECT 
		  PROCEDURE_CODE                                     as                                 DEP_PROCEDURE_CODE
		, FEE_SCHEDULE                                       as                                   DEP_FEE_SCHEDULE 
				FROM     LOGIC_DEP   ), 
RENAME_CL as ( SELECT 
		  CLM_REL_SNPSHT_IND                                 as                                 CLM_REL_SNPSHT_IND
		, CLM_NO                                             as                                             CLM_NO
		, AGRE_ID                                            as                                        CLM_AGRE_ID 
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
				FROM     LOGIC_CL   ), 
RENAME_APC as ( SELECT 
		  INVOICE_LINE_ID                                    as                                APC_INVOICE_LINE_ID
		, STATUS                                             as                                         APC_STATUS
		, APC_CODE                                           as                                           APC_CODE
		, BASE_AMOUNT                                        as                                        BASE_AMOUNT
		, OUTLIER                                            as                                            OUTLIER
		, RURAL                                              as                                              RURAL
		, HOLD_HARMLESS                                      as                                      HOLD_HARMLESS
		, MEDICARE_AMOUNT                                    as                                    MEDICARE_AMOUNT
		, MARKUP                                             as                                             MARKUP
		, CALC_TOTAL                                         as                                         CALC_TOTAL
		, DISCOUNT                                           as                                           DISCOUNT
		, FINAL_MARKUP                                       as                                       FINAL_MARKUP
		, COMPOSITE_ADJUSTMENT                               as                               COMPOSITE_ADJUSTMENT
		, NON_OPPS_BILL_TYPE                                 as                                 NON_OPPS_BILL_TYPE
		, OPPS_FLAG                                          as                                          OPPS_FLAG
		, PAYMENT_METHOD                                     as                                     PAYMENT_METHOD
		, SERVICE_UNITS                                      as                                      SERVICE_UNITS
		, PACKAGING                                          as                                          PACKAGING
		, PAYMENT_ADJUSTMENT                                 as                                 PAYMENT_ADJUSTMENT
		, PAYMENT_INDICATOR                                  as                                  PAYMENT_INDICATOR 
				FROM     LOGIC_APC   ), 
RENAME_DRG as ( SELECT 
		  INVOICE_HEADER_ID                                  as                              DRG_INVOICE_HEADER_ID
		, DRG_NUMBER                                         as                                         DRG_NUMBER 
				FROM     LOGIC_DRG   ), 
RENAME_ID as ( SELECT 
		  INVOICE_HEADER_ID                                  as                               ID_INVOICE_HEADER_ID
		, SEQUENCE_NUMBER                                    as                                    SEQUENCE_NUMBER
		, DIAGNOSIS_CODE                                     as                           ADMITTING_DIAGNOSIS_CODE 
				FROM     LOGIC_ID   ), 
RENAME_CSH as ( SELECT 
		  CLM_STT_TYP_CD                                     as                                     CLM_STT_TYP_CD
		, CLM_STS_TYP_CD                                     as                                     CLM_STS_TYP_CD
		, CLM_TRANS_RSN_TYP_CD                               as                               CLM_TRANS_RSN_TYP_CD
		, HIST_EFF_DT                                        as                             HISTORY_EFFECTIVE_DATE
		, HIST_END_DT                                        as                                   HISTORY_END_DATE
		, CLM_NO                                             as                                   CSH_CLAIM_NUMBER 
				FROM     LOGIC_CSH   ), 
RENAME_DPC as ( SELECT 
		  TCN_NO                                             as                                             TCN_NO
		, WRNT_DATE                                          as                                          WRNT_DATE
		, WRNT_NO                                            as                                            WRNT_NO 
				FROM     LOGIC_DPC   ), 
RENAME_OPPS as ( SELECT 
		  EFFECTIVE_DATE                                     as                                OPPS_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                               OPPS_EXPIRATION_DATE
		, PRO_NUM                                            as                                            PRO_NUM
		, PAY_TO_COST                                        as                                        PAY_TO_COST
		, OUT_CCR                                            as                                            OUT_CCR
		, DEV_CCR                                            as                                            DEV_CCR
		, PUB_DEV_CCR                                        as                                        PUB_DEV_CCR
		, WAGE_INDEX                                         as                                         WAGE_INDEX
		, PROVIDER_CBSA_CODE                                 as                                               CBSA 
				FROM     LOGIC_OPPS   ), 
RENAME_IPPS as ( SELECT 
		  PRO_NUM                                            as                                       IPPS_PRO_NUM
		, EFFECTIVE_DATE                                     as                                IPPS_EFFECTIVE_DATE
		, EXPIRATION_DATE                                    as                               IPPS_EXPIRATION_DATE
		, IN_CCR                                             as                                             IN_CCR
		, DGME                                               as                                               DGME 
				FROM     LOGIC_IPPS   ),
RENAME_CTH as ( SELECT 
		  CLM_NO                                             as                                         CTH_CLM_NO
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
		, HIST_EFF_DT                                        as                                        HIST_EFF_DT
		, HIST_END_DT                                        as                                        HIST_END_DT 
		, CHNG_OVR_IND                                       as                                       CHNG_OVR_IND 
				FROM     LOGIC_CTH   ), 
RENAME_PSR as ( SELECT 
		  PLCY_NO                                            as                                        PSR_PLCY_NO
		, MOST_RCNT_PLCY_STS_RSN_IND                         as                         MOST_RCNT_PLCY_STS_RSN_IND
		, CRNT_PLCY_PRD_STS_RSN_IND                          as                          CRNT_PLCY_PRD_STS_RSN_IND
		, PLCY_TYP_CODE                                      as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
		, PLCY_PRD_EFF_DATE                                  as                                  PLCY_PRD_EFF_DATE 
				FROM     LOGIC_PSR   )				

---- FILTER LAYER (uses aliases) ----
,
FILTER_IL                             as ( SELECT * from    RENAME_IL   ),
FILTER_IH                             as ( SELECT * from    RENAME_IH 
                                            WHERE INVOICE_TYPE IN ('HI','HO')  ),
FILTER_PYC                            as ( SELECT * from    RENAME_PYC   ),
FILTER_DEP                            as ( SELECT * from    RENAME_DEP   ),
FILTER_VR                             as ( SELECT * from    RENAME_VR   ),
FILTER_CL                             as ( SELECT * from    RENAME_CL 
                                            WHERE CLM_REL_SNPSHT_IND = 'N'  ),
FILTER_PART                           as ( SELECT * from    RENAME_PART 
                                            WHERE PART_PTCP_TYP_CD = 'CLMT' AND PART_CP_VOID_IND = 'N'  ),
FILTER_PROVT                          as ( SELECT * from    RENAME_PROVT 
                                            WHERE PROVT_DCTVT_DTTM > current_date  ),
FILTER_NTWK                           as ( SELECT * from    RENAME_NTWK 
                                            WHERE END_DATE IS NULL  ),
FILTER_APC                            as ( SELECT * from    RENAME_APC   ),
FILTER_DRG                            as ( SELECT * from    RENAME_DRG   ),
FILTER_ID                             as ( SELECT * from    RENAME_ID 
                                            WHERE SEQUENCE_NUMBER = 0  ),
FILTER_CTH                            as ( SELECT * from    RENAME_CTH   ),
FILTER_CSH                            as ( SELECT * from    RENAME_CSH   ),
FILTER_OPPS                           as ( SELECT * from    RENAME_OPPS 
                                            WHERE PRO_NUM NOT IN ('12542570001' ,  '12549080001' , '12616060001')  ),
FILTER_DPC                            as ( SELECT * from    RENAME_DPC   
															QUALIFY (ROW_NUMBER() OVER (PARTITION BY TCN_NO ORDER BY WRNT_DATE ASC) ) = 1  ),
FILTER_IPPS                           as ( SELECT * from    RENAME_IPPS   ),
FILTER_PSR                            as ( SELECT * from    RENAME_PSR
                                            WHERE CRNT_PLCY_PRD_STS_RSN_IND = 'Y' and MOST_RCNT_PLCY_STS_RSN_IND = 'Y'
),	  
---- JOIN LAYER ----

CL as ( SELECT * 
				FROM  FILTER_CL
				LEFT JOIN FILTER_PART ON  FILTER_CL.CLM_AGRE_ID =  FILTER_PART.PART_AGRE_ID  ),
IH as ( SELECT * 
				FROM  FILTER_IH
				LEFT JOIN CL ON  FILTER_IH.CLAIM_NUMBER = CL.CLM_NO 
						LEFT JOIN FILTER_PROVT ON  FILTER_IH.PAYTO_PEACH_NUMBER =  FILTER_PROVT.PROVT_PEACH_NUMBER 
								LEFT JOIN FILTER_DRG ON  FILTER_IH.HEADER_INVOICE_HEADER_ID =  FILTER_DRG.DRG_INVOICE_HEADER_ID 
								LEFT JOIN FILTER_ID ON  FILTER_IH.HEADER_INVOICE_HEADER_ID =  FILTER_ID.ID_INVOICE_HEADER_ID
							    LEFT JOIN FILTER_CTH ON  FILTER_IH.CLAIM_NUMBER =  FILTER_CTH.CTH_CLM_NO AND RECEIPT_DATE BETWEEN HIST_EFF_DT  AND coalesce(HIST_END_DT, '2099-12-31') 	 
								LEFT JOIN FILTER_CSH ON  FILTER_IH.CLAIM_NUMBER =  FILTER_CSH.CSH_CLAIM_NUMBER AND RECEIPT_DATE BETWEEN HISTORY_EFFECTIVE_DATE  AND coalesce(HISTORY_END_DATE, '2099-12-31') 
								LEFT JOIN FILTER_OPPS ON  FILTER_IH.SERVICING_PEACH_NUMBER =  FILTER_OPPS.PRO_NUM AND SERVICE_FROM BETWEEN FILTER_OPPS.OPPS_EFFECTIVE_DATE AND FILTER_OPPS.OPPS_EXPIRATION_DATE 
								LEFT JOIN FILTER_DPC ON  FILTER_IH.INVOICE_NUMBER =  FILTER_DPC.TCN_NO 
								LEFT JOIN FILTER_IPPS ON  FILTER_IH.SERVICING_PEACH_NUMBER =  FILTER_IPPS.IPPS_PRO_NUM AND SERVICE_FROM BETWEEN FILTER_IPPS.IPPS_EFFECTIVE_DATE AND FILTER_IPPS.IPPS_EXPIRATION_DATE 
								LEFT JOIN FILTER_PSR ON FILTER_IH.POLICY_NUMBER = FILTER_PSR.PSR_PLCY_NO ),
IL as ( SELECT * 
				FROM  FILTER_IL
				INNER JOIN IH ON  FILTER_IL.LINE_INVOICE_HEADER_ID = IH.HEADER_INVOICE_HEADER_ID 
						LEFT JOIN FILTER_PYC ON  FILTER_IL.PROCEDURE_CODE =  FILTER_PYC.PYC_PROCEDURE_CODE 
								LEFT JOIN FILTER_DEP ON  FILTER_IL.PROCEDURE_CODE =  FILTER_DEP.DEP_PROCEDURE_CODE 
								LEFT JOIN FILTER_VR ON  FILTER_IL.PROCEDURE_CODE =  FILTER_VR.VR_PROCEDURE_CODE 
								LEFT JOIN FILTER_NTWK ON  FILTER_IL.PAID_MCO_ID =  FILTER_NTWK.NTWK_ID 
								LEFT JOIN FILTER_APC ON  FILTER_IL.INVOICE_LINE_ID =  FILTER_APC.APC_INVOICE_LINE_ID  ),
ETL1 as ( SELECT 
md5(cast(
    
    coalesce(cast(INVOICE_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(LINE_SEQUENCE as 
    varchar
), '') || '-' || coalesce(cast(SEQUENCE_EXTENSION as 
    varchar
), '') || '-' || coalesce(cast(LINE_VERSION_NUMBER as 
    varchar
), '') || '-' || coalesce(cast(LINE_DLM as 
    varchar
), '')

 as 
    varchar
)) as UNIQUE_ID_KEY,
md5(cast(
    
    coalesce(cast(MCO_NUMBER as 
    varchar
), '')

 as 
    varchar
)) as MCO_ID_KEY,
* 
FROM IL
QUALIFY (ROW_NUMBER()OVER(PARTITION BY INVOICE_LINE_ID ORDER BY PLCY_PRD_EFF_DATE) ) =1 )

SELECT 
  UNIQUE_ID_KEY
, HEADER_INVOICE_HEADER_ID
, INVOICE_NUMBER
, MCO_NUMBER
, MCO_ID_KEY
, CLAIM_NUMBER
, CUST_NO
, POLICY_NUMBER
, SERVICING_PROVIDER_ID
, SERVICING_PEACH_NUMBER
, PAYTO_PROVIDER_ID
, PAYTO_PEACH_NUMBER
, OTHER_PHYSICIAN1
, PRESCRIBING_PHYISCIAN
, INVOICE_TYPE
, INVOICE_TYPE_DESC
, INVOICE_STATUS
, INVOICE_STATUS_DESC
, HEADER_ENTRY_DATE
, RECEIPT_DATE
, NETWORK_RECEIPT_DATE
, SERVICE_FROM
, SERVICE_TO
, BATCH_NUMBER
, BATCH_SEQUENCE
, BATCH_TYPE
, EXTENSION_NUMBER
, HEADER_VERSION_NUMBER
, PAYEE_ID
, NETWORK_PAYEE_IND
, PAYEE_TYPE
, ADMISSION_DATE
, ADMISSION_HOURS
, ADMISSION_TYPE
, ADMISSION_SOURCE
, DISCHARGE_STATUS
, DISCHARGE_DATE
, DISCHARGE_HOURS
, BILL_DATE
, BILL_TYPE
, PRO_DRG
, FRMTTD_PRO_DRG
, PATIENT_ACCOUNT_NUMBER
, REFERRING_PROVIDER_ID
, REFERRING_PEACH_NUMBER
, HEADER_ULM
, HEADER_DLM
, MOD_SET
, LINE_INVOICE_HEADER_ID
, INVOICE_LINE_ID
, LINE_SEQUENCE
, SEQUENCE_EXTENSION
, LINE_VERSION_NUMBER
, PAID_MCO_ID
, PAID_MCO_NUMBER
, INTEREST_ACCRUAL_DATE
, DATE_OF_SERVICE_FROM
, DATE_OF_SERVICE_TO
, SERVICE_CODE1
, SERVICE_CODE2
, PROCEDURE_CODE
, REVENUE_CENTER_CODE
, NATIONAL_DRUG_CODE
, PLACE_OF_SERVICE_CODE
, PLACE_OF_SERVICE_DESC
, LINE_STATUS_CODE
, LINE_STATUS_DESC
, LINE_STATUS_DATE
, ADJUDICATION_STATUS_CODE
, ADJUDICATION_STATUS_DESC
, ADJUDICATION_STATUS_DATE
, REVERSAL_IND
, OVERRIDE_IND
, DENIED_FLAG
, PAYMENT_NUMBER
, PAYMENT_DATE
, DIAGNOSIS1
, DIAGNOSIS2
, DIAGNOSIS3
, DIAGNOSIS4
, DIAGNOSIS_CODE
, EDI_ID
, LINE_ENTRY_DATE
, ENTRY_USER
, LINE_DLM
, LINE_ULM
, PAID_UNITS
, UNITS_OF_SERVICE
, BILLED_AMOUNT
, CALC_AMOUNT
, NON_COVERED_AMOUNT
, APPROVED_AMOUNT
, NTWK_BILLED_AMT
, FEE_SCHED_AMOUNT
, INTEREST
, PMT_AMT
, nvl(SUBROGATION_FLAG, 'N') SUBROGATION_FLAG
, case when LINE_VERSION_NUMBER <> 0 Then 'LINE ITEM ADJUSTMENT'
	           when EXTENSION_NUMBER = 0 AND LINE_VERSION_NUMBER = 0 Then 'ORIGINAL'
	           when EXTENSION_NUMBER <> 0 AND LINE_VERSION_NUMBER = 0 Then 'FINANCIAL ADJUSTMENT'
	           else NULL end as ADJUSTMENT_TYPE 
, INPUT_METHOD_CODE
, NVL(PAYMENT_CATEGORY, 'OTHER') PAYMENT_CATEGORY
, case when INVOICE_TYPE in ('HO', 'HI', 'AS', 'PS') then INVOICE_TYPE_DESC
       when DEP_FEE_SCHEDULE is not null then DEP_FEE_SCHEDULE 
       when VR_FEE_SCHEDULE is not null then VR_FEE_SCHEDULE
       else 'PROFESSIONAL' END  as 	FEE_SCHEDULE
, PYC_PROCEDURE_CODE
, DEP_PROCEDURE_CODE
, DEP_FEE_SCHEDULE
, VR_PROCEDURE_CODE
, VR_FEE_SCHEDULE
, MOD1_MODIFIER_CODE
, MOD2_MODIFIER_CODE
, MOD3_MODIFIER_CODE
, MOD4_MODIFIER_CODE
, CLM_REL_SNPSHT_IND
, CLM_NO
, CLM_AGRE_ID
, PART_AGRE_ID
, PART_PTCP_TYP_CD
, PART_CP_VOID_IND
, PROVT_PEACH_NUMBER
, PEACH_BASE_NUMBER
, PEACH_SUFFIX_NUMBER
, PAYTO_PRVDR_TYPE_CODE
, PROVT_DCTVT_DTTM
, NTWK_ID
, END_DATE
, APC_INVOICE_LINE_ID
, APC_STATUS
, APC_CODE
, BASE_AMOUNT
, OUTLIER
, RURAL
, HOLD_HARMLESS
, MEDICARE_AMOUNT
, MARKUP
, CALC_TOTAL
, DISCOUNT
, FINAL_MARKUP
, COMPOSITE_ADJUSTMENT
, NON_OPPS_BILL_TYPE
, OPPS_FLAG
, PAYMENT_METHOD
, SERVICE_UNITS
, PACKAGING
, PAYMENT_ADJUSTMENT
, PAYMENT_INDICATOR
, DRG_INVOICE_HEADER_ID
, DRG_NUMBER
, ID_INVOICE_HEADER_ID
, SEQUENCE_NUMBER
, ADMITTING_DIAGNOSIS_CODE
, CLM_STT_TYP_CD
, CLM_STS_TYP_CD
, CLM_TRANS_RSN_TYP_CD
, HISTORY_EFFECTIVE_DATE
, HISTORY_END_DATE
, CSH_CLAIM_NUMBER
, TCN_NO
, WRNT_DATE
, WRNT_NO
, OPPS_EFFECTIVE_DATE
, OPPS_EXPIRATION_DATE
, PRO_NUM
, PAY_TO_COST
, OUT_CCR
, DEV_CCR
, PUB_DEV_CCR
, WAGE_INDEX
, CBSA
, PAID_ABOVE_ZERO_IND
, SUBROGATION_TYPE_DESC
, TOTAL_APPROVED_AMOUNT
, IPPS_PRO_NUM
, IPPS_EFFECTIVE_DATE
, IPPS_EXPIRATION_DATE
, IN_CCR
, DGME
, CTH_CLM_NO
, CLM_TYP_CD
, HIST_EFF_DT
, HIST_END_DT
, CHNG_OVR_IND
, PSR_PLCY_NO
, MOST_RCNT_PLCY_STS_RSN_IND
, CRNT_PLCY_PRD_STS_RSN_IND
, POLICY_TYPE_CODE
, PLCY_STS_TYP_CD
, PLCY_STS_RSN_TYP_CD
, POLICY_ACTIVE_IND
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
, K_PROGRAM_REASON_DESC
 FROM ETL1
      );
    