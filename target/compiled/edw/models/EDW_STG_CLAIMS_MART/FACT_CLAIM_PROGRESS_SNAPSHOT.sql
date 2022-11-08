

---- SRC LAYER ----
WITH
SRC_CLM as ( SELECT *     from     STAGING.DSV_CLAIM ),
SRC_IW as ( SELECT *     from     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
SRC_P_ICD as ( SELECT *     from     EDW_STAGING_DIM.DIM_ICD ),
SRC_EMP as ( SELECT *     from     EDW_STAGING_DIM.DIM_EMPLOYER ),
SRC_MC as ( SELECT *     from     EDW_STAGING_DIM.DIM_MANUAL_CLASSIFICATION ),
SRC_INVEST as ( SELECT *     from     STAGING.DSV_CLAIM_INVESTIGATION ),
SRC_CSPC as ( SELECT *     from     STAGING.DSV_CLAIM_PAYMENT_SUMMARY ),
SRC_INV as ( SELECT *     from     STAGING.DSV_INVOICE ),
SRC_NTWK as ( SELECT *     from     EDW_STAGING_DIM.DIM_NETWORK ),
SRC_DM as ( SELECT *     from     STAGING.DSV_DISABILITY_MANAGEMENT ),
SRC_IPS            as ( SELECT *     from     STAGING.DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT ),
//SRC_CLM as ( SELECT *     from     DSV_CLAIM) ,
//SRC_IW as ( SELECT *     from     DIM_INJURED_WORKER) ,
//SRC_P_ICD as ( SELECT *     from     DIM_ICD) ,
//SRC_EMP as ( SELECT *     from     DIM_EMPLOYER) ,
//SRC_MC as ( SELECT *     from     DIM_MANUAL_CLASSIFICATION) ,
//SRC_INVEST as ( SELECT *     from     DSV_CLAIM_INVESTIGATION) ,
//SRC_CSPC as ( SELECT *     from     DSV_CLAIM_PAYMENT_SUMMARY) ,
//SRC_INV as ( SELECT *     from     DSV_INVOICE) ,
//SRC_NTWK as ( SELECT *     from     DIM_NETWORK) ,
//SRC_DM as ( SELECT *     from     DSV_DISABILITY_MANAGEMENT) ,
//SRC_IPS            as ( SELECT *     from     DSV_INDEMNITY_PLAN_SCHEDULE_DETAIL_PAYMENT) ,

---- LOGIC LAYER ----


LOGIC_CLM as ( SELECT 

  CLAIM_NUMBER                                      as                                        CLAIM_NUMBER
, AGRE_ID                                           as                                             AGRE_ID 
, MCO_NO                                            as                                              MCO_NO
, CASE WHEN  POLICY_ORIGINAL_EFFECTIVE_DATE is null then '-1' 
      WHEN  POLICY_ORIGINAL_EFFECTIVE_DATE < '1901-01-01' then '-2' 
      WHEN  POLICY_ORIGINAL_EFFECTIVE_DATE > '2099-12-31' then '-3' 
          ELSE regexp_replace(  POLICY_ORIGINAL_EFFECTIVE_DATE, '[^0-9]+', '') 
			END :: INTEGER                          as                                POLICY_ORIGINAL_EFFECTIVE_DATE_KEY
, CASE WHEN  CLAIM_INITIAL_FILE_DATE is null then '-1' 
      WHEN  CLAIM_INITIAL_FILE_DATE < '1901-01-01' then '-2' 
      WHEN  CLAIM_INITIAL_FILE_DATE > '2099-12-31' then '-3' 
          ELSE regexp_replace(  CLAIM_INITIAL_FILE_DATE, '[^0-9]+', '') 
			END :: INTEGER                          as                                INITIAL_FILE_DATE_KEY 
, CASE WHEN CLAIM_FILE_DATE is null then '-1' 
      WHEN CLAIM_FILE_DATE < '1901-01-01' then '-2' 
      WHEN CLAIM_FILE_DATE > '2099-12-31' then '-3' 
          ELSE regexp_replace( CLAIM_FILE_DATE, '[^0-9]+', '') 
			END :: INTEGER                          as                                        FILE_DATE_KEY 
,CASE WHEN CLM_OCCR_DATE is null then '-1' 
	  WHEN CLM_OCCR_DATE < '1901-01-01' then '-2' 
	  WHEN CLM_OCCR_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_OCCR_DATE, '[^0-9]+', '') 
				END :: INTEGER                      as                                  OCCURRENCE_DATE_KEY 
 
, CASE WHEN INDUSTRY_GROUP_CODE is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(INDUSTRY_GROUP_CODE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                    as   	                            INDUSTRY_GROUP_HKEY 
, MD5( '99999' )                                    as   		                          FILING_PARTY_HKEY 
,  md5(cast(
    
    coalesce(cast(CLM_ENTRY_USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
)) as                         ENTRY_USER_HKEY 
, case when CLM_AUDIT_USER_CREA_DTM  is null then -1
    when replace(cast(CLM_AUDIT_USER_CREA_DTM ::DATE as varchar),'-','')::INTEGER  < 19010101 then -2
    when replace(cast(CLM_AUDIT_USER_CREA_DTM ::DATE as varchar),'-','')::INTEGER  > 20991231 then -3
    else replace(cast(CLM_AUDIT_USER_CREA_DTM 	 ::DATE as varchar),'-','')::INTEGER 
        END                                         as                        SOURCE_SYSTEM_CREATE_DATE_KEY 
, CASE WHEN CLAIM_RELEASE_DATE is null then '-1' 
			WHEN CLAIM_RELEASE_DATE < '1901-01-01' then '-2' 
			WHEN CLAIM_RELEASE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLAIM_RELEASE_DATE, '[^0-9]+', '') 
				END :: INTEGER                       as                              CLAIM_RELEASE_DATE_KEY 
, CASE WHEN CLM_FIRST_DECISION_DATE is null then '-1' 
			WHEN CLM_FIRST_DECISION_DATE < '1901-01-01' then '-2' 
			WHEN CLM_FIRST_DECISION_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CLM_FIRST_DECISION_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                          FIRST_DETERMINATION_DATE_KEY 
, CASE WHEN DETERMINATION_USER_LGN_NM IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
               ELSE md5(cast(
    
    coalesce(cast(DETERMINATION_USER_LGN_NM as 
    varchar
), '')

 as 
    varchar
))    
			   END                                 as                               DETERMINATION_USER_HKEY 

, CASE WHEN ORG_UNT_NM IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
               ELSE md5(cast(
    
    coalesce(cast(ORG_UNT_NM as 
    varchar
), '')

 as 
    varchar
))   
			    END                                as                              ORGANIZATIONAL_UNIT_HKEY 
, CASE WHEN FIRST_ASSIGNMENT_DATE is null then '-1' 
			WHEN FIRST_ASSIGNMENT_DATE < '1901-01-01' then '-2' 
			WHEN FIRST_ASSIGNMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( FIRST_ASSIGNMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                              FIRST_ASSIGNMENT_DATE_KEY 
 
, CASE WHEN LAST_ASSIGNMENT_DATE is null then '-1' 
			WHEN LAST_ASSIGNMENT_DATE < '1901-01-01' then '-2' 
			WHEN LAST_ASSIGNMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( LAST_ASSIGNMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                               LAST_ASSIGNMENT_DATE_KEY 
, CASE WHEN FIRST_POR_VISIT_DATE is null then '-1' 
			WHEN FIRST_POR_VISIT_DATE < '1901-01-01' then '-2' 
			WHEN FIRST_POR_VISIT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( FIRST_POR_VISIT_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                               FIRST_POR_VISIT_DATE_KEY 
, CASE WHEN nullif(array_to_string(array_construct_compact( POLICY_TYPE_CODE, PLCY_STS_TYP_CD, PLCY_STS_RSN_TYP_CD, POLICY_ACTIVE_IND),''), '') is NULL  
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
            END                                    as                                   POLICY_STANDING_HKEY	
, CASE WHEN LAST_DAY_AT_WORK_DATE is null then '-1' 
			WHEN LAST_DAY_AT_WORK_DATE < '1901-01-01' then '-2' 
			WHEN LAST_DAY_AT_WORK_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( LAST_DAY_AT_WORK_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                             LAST_DAY_AT_WORK_DATE_KEY
, CASE WHEN ESTIMATED_RETURN_TO_WORK_DATE is null then '-1' 
			WHEN ESTIMATED_RETURN_TO_WORK_DATE < '1901-01-01' then '-2' 
			WHEN ESTIMATED_RETURN_TO_WORK_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( ESTIMATED_RETURN_TO_WORK_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                     ESTIMATED_RETURN_TO_WORK_DATE_KEY 
, CASE WHEN ACTUAL_RETURN_TO_WORK_DATE is null then '-1' 
			WHEN ACTUAL_RETURN_TO_WORK_DATE < '1901-01-01' then '-2' 
			WHEN ACTUAL_RETURN_TO_WORK_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( ACTUAL_RETURN_TO_WORK_DATE, '[^0-9]+', '') 
				END :: INTEGER                     as                        ACTUAL_RETURN_TO_WORK_DATE_KEY 
, CASE WHEN  nullif(array_to_string(array_construct_compact( CURRENT_CORESUITE_CLAIM_TYPE_CODE, CLAIM_TYPE_CHNG_OVR_IND, CLAIM_STATE_CODE, CLAIM_STATUS_CODE, CLAIM_STATUS_REASON_CODE ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CURRENT_CORESUITE_CLAIM_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CLAIM_TYPE_CHNG_OVR_IND as 
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
				END                                 as    				             CLAIM_TYPE_STATUS_HKEY 
,  md5(cast(
    
    coalesce(cast(FILING_SOURCE_DESC as 
    varchar
), '') || '-' || coalesce(cast(FILING_MEDIA_DESC as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_CATEGORY as 
    varchar
), '') || '-' || coalesce(cast(NATURE_OF_INJURY_TYPE as 
    varchar
), '') || '-' || coalesce(cast(FIREFIGHTER_CANCER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EXPOSURE_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_EMERGENCY_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COVID_HEALTH_CARE_WORKER_IND as 
    varchar
), '') || '-' || coalesce(cast(COMBINED_IND as 
    varchar
), '') || '-' || coalesce(cast(SB223_IND as 
    varchar
), '') || '-' || coalesce(cast(EMPLOYER_PREMISES_IND as 
    varchar
), '') || '-' || coalesce(cast(CATASTROPHIC_IND as 
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
                                                    as       			                  CLAIM_DETAIL_HKEY 
, CASE WHEN  nullif(array_to_string(array_construct_compact( ACCIDENT_DESCRIPTION_TEXT, IW_JOB_TITLE ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(ACCIDENT_DESCRIPTION_TEXT as 
    varchar
), '') || '-' || coalesce(cast(IW_JOB_TITLE as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                    as                             CLAIM_ACCIDENT_DESC_HKEY 
		, CASE WHEN EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE is null then '-1' 
			WHEN EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                    as             EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE_KEY 
		, CASE WHEN EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE is null then '-1' 
			WHEN EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE < '1901-01-01' then '-2' 
			WHEN EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                    as           EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE_KEY 
, CASE WHEN CATASTROPHIC_INJURY_TYPE_CODE IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
               ELSE md5(cast(
    
    coalesce(cast(CATASTROPHIC_INJURY_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
))    END 
			                                        as                       CATASTROPHIC_INJURY_TYPE_HKEY 
		, CASE WHEN CATASTROPHIC_EFFECTIVE_DATE is null then '-1' 
			WHEN CATASTROPHIC_EFFECTIVE_DATE < '1901-01-01' then '-2' 
			WHEN CATASTROPHIC_EFFECTIVE_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CATASTROPHIC_EFFECTIVE_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                    as                     CATASTROPHIC_EFFECTIVE_DATE_KEY 
		, CASE WHEN CATASTROPHIC_EXPIRATION_DATE is null then '-1' 
			WHEN CATASTROPHIC_EXPIRATION_DATE < '1901-01-01' then '-2' 
			WHEN CATASTROPHIC_EXPIRATION_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( CATASTROPHIC_EXPIRATION_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                     as                   CATASTROPHIC_EXPIRATION_DATE_KEY 
, CASE WHEN EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE is null then '-1' 
			WHEN EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE < '1901-01-01' then '-2' 
			WHEN EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE, '[^0-9]+', '') 
				END :: INTEGER                                         
                                                     as     EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE_KEY 
, POLICY_NUMBER                                      as                                      POLICY_NUMBER 
, BUSINESS_SEQ_NUMBER                                as                                BUSINESS_SEQ_NUMBER 
, IW_AGE_AT_OCCURRENCE                               as                               IW_AGE_AT_OCCURRENCE 
, CLAIM_FILE_LAG_DAYS_COUNT                          as                          CLAIM_FILE_LAG_DAYS_COUNT 
, DATEDIFF(DAYS, CLAIM_FILE_DATE , CLM_FIRST_DECISION_DATE ) as FROI_TO_FIRST_DATE_OF_DETERMINATION_LAG_DAY_COUNT
, DATEDIFF(DAYS, CLAIM_FILE_DATE , CLAIM_CLOSED_DATE ) as                      FROI_TO_CLOSED_LAG_DAY_COUNT 
, INVOICE_DISTINCT_COUNT                             as 							 INVOICE_DISTINCT_COUNT 
, INVOICE_LINE_DISTINCT_COUNT 						 as 						INVOICE_LINE_DISTINCT_COUNT 
, TOTAL_SCH_LOSS_PAID_AMT                            as                             TOTAL_SCH_LOSS_PAID_AMT 
, TOTAL_NWWL_PAID_AMT                                as                                 TOTAL_NWWL_PAID_AMT 
, CLAIM_FILE_DATE	                                 as	                                    CLAIM_FILE_DATE 
, IW_CUSTOMER_NUMBER	                             as	                                 IW_CUSTOMER_NUMBER 
, PRIMARY_ICD_CODE	                                 as	                                   PRIMARY_ICD_CODE 
, EMP_CUSTOMER_NUMBER	                             as	                                EMP_CUSTOMER_NUMBER 
, MANUAL_CLASS_CODE	                                 as	                                  MANUAL_CLASS_CODE 
, DRVD_MANUAL_CLASS_SUFFIX_CODE	                     as	                      DRVD_MANUAL_CLASS_SUFFIX_CODE 
, DETERMINATION_USER_LGN_NM	                         as	                          DETERMINATION_USER_LGN_NM 
, ORG_UNT_NM	                                     as	                                         ORG_UNT_NM 
, INTL_STLD_INDM_DATE	                             as	                                INTL_STLD_INDM_DATE 
, INTL_STLD_MDCL_DATE	                             as	                                INTL_STLD_MDCL_DATE 
, CLM_CLMT_NTF_DT	                                 as	                                    CLM_CLMT_NTF_DT 
, CLM_EMPLR_NTF_DT	                                 as	                                   CLM_EMPLR_NTF_DT 
, K_PROGRAM_ENROLLMENT_DESC	                         as	                          K_PROGRAM_ENROLLMENT_DESC 
, K_PROGRAM_TYPE_DESC	                             as	                                K_PROGRAM_TYPE_DESC 
, K_PROGRAM_REASON_CODE	                             as	                              K_PROGRAM_REASON_CODE 
, FILING_SOURCE_DESC	                             as	                                 FILING_SOURCE_DESC 
, FILING_MEDIA_DESC	                                 as	                                  FILING_MEDIA_DESC 
, NATURE_OF_INJURY_CATEGORY	                         as	                          NATURE_OF_INJURY_CATEGORY 
, NATURE_OF_INJURY_TYPE	                             as	                              NATURE_OF_INJURY_TYPE 
, FIREFIGHTER_CANCER_IND	                         as	                             FIREFIGHTER_CANCER_IND 
, COVID_EXPOSURE_IND	                             as	                                 COVID_EXPOSURE_IND 
, COVID_EMERGENCY_WORKER_IND	                     as	                         COVID_EMERGENCY_WORKER_IND 
, COVID_HEALTH_CARE_WORKER_IND	                     as	                       COVID_HEALTH_CARE_WORKER_IND 
, COMBINED_IND	                                     as	                                       COMBINED_IND 
, SB223_IND	                                         as	                                          SB223_IND 
, EMPLOYER_PREMISES_IND	                             as	                              EMPLOYER_PREMISES_IND 
, CATASTROPHIC_IND	                                 as	                                   CATASTROPHIC_IND 
, CURRENT_CORESUITE_CLAIM_TYPE_CODE	                 as	                  CURRENT_CORESUITE_CLAIM_TYPE_CODE 
, CLAIM_STATE_CODE	                                 as	                                   CLAIM_STATE_CODE 
, CLAIM_STATUS_CODE	                                 as	                                  CLAIM_STATUS_CODE 
, CLAIM_STATUS_REASON_CODE	                         as	                           CLAIM_STATUS_REASON_CODE 
, CLAIM_TYPE_CHNG_OVR_IND	                         as	                            CLAIM_TYPE_CHNG_OVR_IND 
, ACCIDENT_DESCRIPTION_TEXT	                         as	                          ACCIDENT_DESCRIPTION_TEXT 
, IW_JOB_TITLE	                                     as                                        IW_JOB_TITLE 
, DATEDIFF(DAYS, CLAIM_FILE_DATE , ACTUAL_RETURN_TO_WORK_DATE) as                FROI_TO_ARTW_LAG_DAY_COUNT 
, DATEDIFF(DAYS, CLAIM_FILE_DATE , NULLIF(least(NVL(INTL_STLD_INDM_DATE,'2099-12-31'), NVL(INTL_STLD_MDCL_DATE,'2099-12-31')),'2099-12-31' )) 
                                                     as                    FROI_TO_SETTLEMENT_LAG_DAY_COUNT 
, DATEDIFF(DAYS, CLAIM_FILE_DATE, CLM_CLMT_NTF_DT)   as              FROI_TO_FIRST_IW_CONTACT_LAG_DAY_COUNT 
, DATEDIFF(DAYS, CLAIM_FILE_DATE, CLM_EMPLR_NTF_DT)  as        FROI_TO_FIRST_EMPLOYER_CONTACT_LAG_DAY_COUNT 
from SRC_CLM
            ),
LOGIC_IW as ( SELECT 
INJURED_WORKER_HKEY                                  as                                INJURED_WORKER_HKEY 
, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 

		from SRC_IW
            ),
LOGIC_P_ICD as ( SELECT 
 ICD_HKEY                                            as                                           ICD_HKEY 
, ICD_CODE                                           as                                           ICD_CODE 
, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_P_ICD
            ),
LOGIC_EMP as ( SELECT 
 COALESCE( EMPLOYER_HKEY, MD5( '99999' ) )           as                                      EMPLOYER_HKEY 
, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_EMP
            ),
LOGIC_MC as ( SELECT 
  MANUAL_CLASS_HKEY                                  as                                  MANUAL_CLASS_HKEY 
, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
, MANUAL_CLASS_CODE                                  as                                  MANUAL_CLASS_CODE 
, MANUAL_CLASS_SUFFIX_CODE                           as                           MANUAL_CLASS_SUFFIX_CODE 
				from SRC_MC
            ),
LOGIC_INVEST as ( SELECT 
CASE WHEN ACP_START_DATE is null then '-1' 
               WHEN ACP_START_DATE < '1901-01-01' then '-2' 
               WHEN ACP_START_DATE > '2099-12-31' then '-3' 
                    ELSE regexp_replace( ACP_START_DATE, '[^0-9]+', '') 
			         END :: INTEGER                  as                                 ACP_START_DATE_KEY
, CASE WHEN ACP_END_DATE is null then '-1' 
               WHEN ACP_END_DATE < '1901-01-01' then '-2' 
               WHEN ACP_END_DATE > '2099-12-31' then '-3' 
                    ELSE regexp_replace( ACP_END_DATE, '[^0-9]+', '') 
			         END :: INTEGER                  as                                   ACP_END_DATE_KEY
, CASE WHEN USER_LOGIN_NAME IS NULL THEN md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
               ELSE md5(cast(
    
    coalesce(cast(USER_LOGIN_NAME as 
    varchar
), '')

 as 
    varchar
))    
			         END                             as                                   PRIMARY_CSS_HKEY
,  md5(cast(
    
    coalesce(cast(CLAIM_ACP_STATUS_IND as 
    varchar
), '') || '-' || coalesce(cast(ACP_MANUAL_INTERVENTION_IND as 
    varchar
), '') || '-' || coalesce(cast(JURISDICTION_TYPE_CODE as 
    varchar
), '')

 as 
    varchar
)) 
                                                     as                                 INVESTIGATION_HKEY
, DATEDIFF (DAYS, ACP_START_DATE, ACP_END_DATE)      as                       ACP_VALIDATION_LAG_DAY_COUNT
, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		from SRC_INVEST
            ),
LOGIC_CSPC as ( SELECT 
FIRST_MEDICAL_PAYMENT_DATE                           as                         FIRST_MEDICAL_PAYMENT_DATE
,CASE WHEN FIRST_MEDICAL_PAYMENT_DATE is null then '-1' 
			WHEN FIRST_MEDICAL_PAYMENT_DATE < '1901-01-01' then '-2' 
			WHEN FIRST_MEDICAL_PAYMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( FIRST_MEDICAL_PAYMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                       as                     FIRST_MEDICAL_PAYMENT_DATE_KEY
, 		  CASE WHEN LAST_MEDICAL_PAYMENT_DATE is null then '-1' 
			WHEN LAST_MEDICAL_PAYMENT_DATE < '1901-01-01' then '-2' 
			WHEN LAST_MEDICAL_PAYMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( LAST_MEDICAL_PAYMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                      as                       LAST_MEDICAL_PAYMENT_DATE_KEY
, FIRST_INDEMNITY_PAYMENT_DATE
, CASE WHEN FIRST_INDEMNITY_PAYMENT_DATE is null then '-1' 
			WHEN FIRST_INDEMNITY_PAYMENT_DATE < '1901-01-01' then '-2' 
			WHEN FIRST_INDEMNITY_PAYMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( FIRST_INDEMNITY_PAYMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                      as                     FIRST_INDEMNITY_PAYMENT_DATE_KEY
, CASE WHEN LAST_INDEMNITY_PAYMENT_DATE is null then '-1' 
			WHEN LAST_INDEMNITY_PAYMENT_DATE < '1901-01-01' then '-2' 
			WHEN LAST_INDEMNITY_PAYMENT_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( LAST_INDEMNITY_PAYMENT_DATE, '[^0-9]+', '') 
				END :: INTEGER                      as                     LAST_INDEMNITY_PAYMENT_DATE_KEY
, CASE WHEN PAYMENT_AMOUNTS_AS_OF_DATE is null then '-1' 
			WHEN PAYMENT_AMOUNTS_AS_OF_DATE < '1901-01-01' then '-2' 
			WHEN PAYMENT_AMOUNTS_AS_OF_DATE > '2099-12-31' then '-3' 
			ELSE regexp_replace( PAYMENT_AMOUNTS_AS_OF_DATE, '[^0-9]+', '') 
				END :: INTEGER                       as                     PAYMENT_AMOUNTS_AS_OF_DATE_KEY
, MEDICAL_HOSPITAL_AMOUNT                            as                            MEDICAL_HOSPITAL_AMOUNT
, MEDICAL_CLINIC_AMOUNT                              as                              MEDICAL_CLINIC_AMOUNT
, MEDICAL_DOCTOR_AMOUNT                              as                              MEDICAL_DOCTOR_AMOUNT
, MEDICAL_NURSING_SERVICES_AMOUNT                    as                    MEDICAL_NURSING_SERVICES_AMOUNT
, MEDICAL_DRUGS_PHARMACY_AMOUNT                      as                      MEDICAL_DRUGS_PHARMACY_AMOUNT
, MEDICAL_XRAY_RADIOLOGY_AMOUNT                      as                      MEDICAL_XRAY_RADIOLOGY_AMOUNT
, MEDICAL_LAB_PATHOLOGY_AMOUNT                       as                       MEDICAL_LAB_PATHOLOGY_AMOUNT
, MEDICAL_MISC_AMOUNT                                as                                MEDICAL_MISC_AMOUNT
, MEDICAL_OTHER_AMOUNT                               as                               MEDICAL_OTHER_AMOUNT
, MEDICAL_FUNERAL_AMOUNT                             as                             MEDICAL_FUNERAL_AMOUNT
, MEDICAL_COURT_AMOUNT                               as                               MEDICAL_COURT_AMOUNT
, TOTAL_MEDICAL_PAID_AMOUNT                          as                          TOTAL_MEDICAL_PAID_AMOUNT
, INDEMNITY_DWRF_AMOUNT                              as                              INDEMNITY_DWRF_AMOUNT
, INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT              as              INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT
, INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT               as               INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT
, INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT                  as                  INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT
, INDEMNITY_PTD_AMOUNT                               as                               INDEMNITY_PTD_AMOUNT
, INDEMNITY_TEMPORARY_TOTAL_AMOUNT                   as                   INDEMNITY_TEMPORARY_TOTAL_AMOUNT
, INDEMNITY_TEMPORARY_PARTIAL_AMOUNT                 as                 INDEMNITY_TEMPORARY_PARTIAL_AMOUNT
, INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT             as             INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT
, INDEMNITY_DEATH_BENEFIT_AMOUNT                     as                     INDEMNITY_DEATH_BENEFIT_AMOUNT
, INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT          as          INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT
, INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT      as      INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT
, INDEMNITY_OTHER_INDEMNITY_AMOUNT                   as                   INDEMNITY_OTHER_INDEMNITY_AMOUNT
, TOTAL_INDEMNITY_PAID_AMOUNT                        as                        TOTAL_INDEMNITY_PAID_AMOUNT
, TOTAL_CLAIM_PAYMENT_AMOUNT                         as                         TOTAL_CLAIM_PAYMENT_AMOUNT
, INDEMNITY_WORKING_WAGE_LOSS_AMOUNT                 as                 INDEMNITY_WORKING_WAGE_LOSS_AMOUNT
, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		from SRC_CSPC
            ),
LOGIC_INV as ( SELECT 
         CLAIM_NUMBER                                as                                       CLAIM_NUMBER
, MIN(CASE WHEN DATE_OF_SERVICE_FROM is null then '-1' 
			WHEN DATE_OF_SERVICE_FROM < '1901-01-01' then '-2' 
			WHEN DATE_OF_SERVICE_FROM > '2099-12-31' then '-3' 
			ELSE regexp_replace( DATE_OF_SERVICE_FROM , '[^0-9]+', '') 
				END :: INTEGER )                                        
                                                    as                      FIRST_MEDICAL_SERVICE_DATE_KEY
        , MAX(CASE WHEN DATE_OF_SERVICE_TO is null then '-1' 
			WHEN DATE_OF_SERVICE_TO < '1901-01-01' then '-2' 
			WHEN DATE_OF_SERVICE_TO > '2099-12-31' then '-3' 
			ELSE regexp_replace( DATE_OF_SERVICE_TO , '[^0-9]+', '') 
				END :: INTEGER  )                                       
                                                    as                       LAST_MEDICAL_SERVICE_DATE_KEY
         from SRC_INV
             group by CLAIM_NUMBER
            ),
LOGIC_NTWK as ( SELECT 
		  COALESCE( NETWORK_HKEY, MD5( '99999' ) )   as                                       NETWORK_HKEY
, NETWORK_NUMBER                                     as                                     NETWORK_NUMBER
, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE
, RECORD_END_DATE                                    as                                    RECORD_END_DATE
		from SRC_NTWK
            ),
LOGIC_DM as ( SELECT 
		 CLAIM_NUMBER                               as                                        CLAIM_NUMBER
,  CASE WHEN  nullif(array_to_string(array_construct_compact( DISABILITY_TYPE_CODE, DISABILITY_REASON_TYPE_CODE, DISABILITY_MEDICAL_STATUS_TYPE_CODE, DISABILITY_WORK_STATUS_TYPE_CODE, CURRENT_DISABILITY_STATUS_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(DISABILITY_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(DISABILITY_REASON_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(DISABILITY_MEDICAL_STATUS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(DISABILITY_WORK_STATUS_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_DISABILITY_STATUS_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         
                                                    as                                DISABILITY_TYPE_HKEY
  	,  SUM(DISABILITY_ACTUAL_ELAPSED_DAY_COUNT)    over ( Partition by  CLAIM_NUMBER )  
	                                                as                              TOTAL_MISSED_DAY_COUNT
		from SRC_DM
		QUALIFY ROW_NUMBER () OVER (PARTITION BY CLAIM_NUMBER order by CURRENT_DISABILITY_STATUS_IND desc, DISABILITY_START_DATE desc, DISABILITY_MANAGEMENT_ID desc ) =1
            ),

LOGIC_IPS as ( SELECT DISTINCT
  PERMANENT_PARTIAL_PERCENT                          as                          PERMANENT_PARTIAL_PERCENT
, INDEMNITY_BENEFIT_PLAN_WEEK_COUNT                  as                  INDEMNITY_BENEFIT_PLAN_WEEK_COUNT
, BENEFIT_TYPE_DESC                                  as                                  BENEFIT_TYPE_DESC
, IP_VOID_IND                                        as                                        IP_VOID_IND
, ISS_VOID_IND                                       as                                       ISS_VOID_IND
, ISD_VOID_IND                                       as                                       ISD_VOID_IND
, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND
, AGRE_ID                                            as                                            AGRE_ID
, INDEMNITY_PLAN_ID                                  as                                  INDEMNITY_PLAN_ID
		FROM SRC_IPS
            )
							
---- RENAME LAYER ----
,
RENAME_CLM as ( SELECT 
  CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
, AGRE_ID                                           as                                         CLM_AGRE_ID
, MCO_NO                                             as                                             MCO_NO
, POLICY_ORIGINAL_EFFECTIVE_DATE_KEY                 AS                 POLICY_ORIGINAL_EFFECTIVE_DATE_KEY
, INITIAL_FILE_DATE_KEY                              as                              INITIAL_FILE_DATE_KEY
, FILE_DATE_KEY                                      as                                      FILE_DATE_KEY
, OCCURRENCE_DATE_KEY                                as                                OCCURRENCE_DATE_KEY
, INDUSTRY_GROUP_HKEY                                as                                INDUSTRY_GROUP_HKEY
, FILING_PARTY_HKEY                                  as                                  FILING_PARTY_HKEY
, ENTRY_USER_HKEY                                    as                                    ENTRY_USER_HKEY
, SOURCE_SYSTEM_CREATE_DATE_KEY                      as                      SOURCE_SYSTEM_CREATE_DATE_KEY
, CLAIM_RELEASE_DATE_KEY                             as                             CLAIM_RELEASE_DATE_KEY
, FIRST_DETERMINATION_DATE_KEY                       as                       FIRST_DETERMINATION_DATE_KEY
, DETERMINATION_USER_HKEY                            as                            DETERMINATION_USER_HKEY
, ORGANIZATIONAL_UNIT_HKEY                           as                           ORGANIZATIONAL_UNIT_HKEY
, FIRST_ASSIGNMENT_DATE_KEY                          as                          FIRST_ASSIGNMENT_DATE_KEY
, LAST_ASSIGNMENT_DATE_KEY                           as                           LAST_ASSIGNMENT_DATE_KEY
, FIRST_POR_VISIT_DATE_KEY                           as                           FIRST_POR_VISIT_DATE_KEY
, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY
, LAST_DAY_AT_WORK_DATE_KEY                          as                          LAST_DAY_AT_WORK_DATE_KEY
, ESTIMATED_RETURN_TO_WORK_DATE_KEY                  as                  ESTIMATED_RETURN_TO_WORK_DATE_KEY
, ACTUAL_RETURN_TO_WORK_DATE_KEY                     as                     ACTUAL_RETURN_TO_WORK_DATE_KEY
, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY
, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY
, CLAIM_ACCIDENT_DESC_HKEY                           as                           CLAIM_ACCIDENT_DESC_HKEY
, EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE_KEY           as           EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE_KEY
, EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE_KEY          as          EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE_KEY
, CATASTROPHIC_INJURY_TYPE_HKEY                      as                      CATASTROPHIC_INJURY_TYPE_HKEY
, CATASTROPHIC_EFFECTIVE_DATE_KEY                    as                    CATASTROPHIC_EFFECTIVE_DATE_KEY
, CATASTROPHIC_EXPIRATION_DATE_KEY                   as                   CATASTROPHIC_EXPIRATION_DATE_KEY
, EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE_KEY     as     EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE_KEY
, POLICY_NUMBER                                      as                                      POLICY_NUMBER
, BUSINESS_SEQ_NUMBER                                as                           BUSINESS_SEQUENCE_NUMBER
, IW_AGE_AT_OCCURRENCE                               as                                  AGE_AT_OCCURRENCE
, CLAIM_FILE_LAG_DAYS_COUNT                          as                                 FILE_LAG_DAY_COUNT
, FROI_TO_FIRST_DATE_OF_DETERMINATION_LAG_DAY_COUNT  as  FROI_TO_FIRST_DATE_OF_DETERMINATION_LAG_DAY_COUNT
, FROI_TO_CLOSED_LAG_DAY_COUNT                       as                       FROI_TO_CLOSED_LAG_DAY_COUNT
, INVOICE_DISTINCT_COUNT                             as                             INVOICE_DISTINCT_COUNT
, INVOICE_LINE_DISTINCT_COUNT                        as                        INVOICE_LINE_DISTINCT_COUNT
, TOTAL_SCH_LOSS_PAID_AMT                            as                     INDEMNITY_SCHEDULE_LOSS_AMOUNT
, TOTAL_NWWL_PAID_AMT                                as             INDEMNITY_NON_WORKING_WAGE_LOSS_AMOUNT
, CLAIM_FILE_DATE	                                 as	                                   CLAIM_FILE_DATE
, IW_CUSTOMER_NUMBER	                             as	                                IW_CUSTOMER_NUMBER
, PRIMARY_ICD_CODE	                                 as	                                  PRIMARY_ICD_CODE
, EMP_CUSTOMER_NUMBER	                             as	                               EMP_CUSTOMER_NUMBER
, MANUAL_CLASS_CODE	                                 as	                             CLM_MANUAL_CLASS_CODE
, DRVD_MANUAL_CLASS_SUFFIX_CODE	                     as	                     DRVD_MANUAL_CLASS_SUFFIX_CODE
, DETERMINATION_USER_LGN_NM                          as	                         DETERMINATION_USER_LGN_NM
, ORG_UNT_NM	                                     as	                                        ORG_UNT_NM
, INTL_STLD_INDM_DATE	                             as	                               INTL_STLD_INDM_DATE
, INTL_STLD_MDCL_DATE	                             as	                               INTL_STLD_MDCL_DATE
, CLM_CLMT_NTF_DT	                                 as	                                   CLM_CLMT_NTF_DT
, CLM_EMPLR_NTF_DT	                                 as	                                  CLM_EMPLR_NTF_DT
, K_PROGRAM_ENROLLMENT_DESC	                         as	                         K_PROGRAM_ENROLLMENT_DESC
, K_PROGRAM_TYPE_DESC	                             as                                K_PROGRAM_TYPE_DESC
, K_PROGRAM_REASON_CODE	                             as	                             K_PROGRAM_REASON_CODE
, FILING_SOURCE_DESC	                             as	                                FILING_SOURCE_DESC
, FILING_MEDIA_DESC	                                 as	                                 FILING_MEDIA_DESC
, NATURE_OF_INJURY_CATEGORY	                         as                          NATURE_OF_INJURY_CATEGORY
, NATURE_OF_INJURY_TYPE	                             as                              NATURE_OF_INJURY_TYPE
, FIREFIGHTER_CANCER_IND	                         as	                            FIREFIGHTER_CANCER_IND
, COVID_EXPOSURE_IND	                             as                                 COVID_EXPOSURE_IND
, COVID_EMERGENCY_WORKER_IND	                     as	                        COVID_EMERGENCY_WORKER_IND
, COVID_HEALTH_CARE_WORKER_IND	                     as	                      COVID_HEALTH_CARE_WORKER_IND
, COMBINED_IND	                                     as	                                      COMBINED_IND
, SB223_IND	                                         as	                                         SB223_IND
, EMPLOYER_PREMISES_IND	                             as	                             EMPLOYER_PREMISES_IND
, CATASTROPHIC_IND	                                 as	                                  CATASTROPHIC_IND
, CURRENT_CORESUITE_CLAIM_TYPE_CODE	                 as	                 CURRENT_CORESUITE_CLAIM_TYPE_CODE
, CLAIM_STATE_CODE	                                 as	                                  CLAIM_STATE_CODE
, CLAIM_STATUS_CODE	                                 as	                                 CLAIM_STATUS_CODE
, CLAIM_STATUS_REASON_CODE	                         as	                          CLAIM_STATUS_REASON_CODE
, CLAIM_TYPE_CHNG_OVR_IND	                         as	                           CLAIM_TYPE_CHNG_OVR_IND
, ACCIDENT_DESCRIPTION_TEXT	                         as	                         ACCIDENT_DESCRIPTION_TEXT
, IW_JOB_TITLE	                                     as	                                      IW_JOB_TITLE
, FROI_TO_ARTW_LAG_DAY_COUNT                         as                         FROI_TO_ARTW_LAG_DAY_COUNT
, FROI_TO_SETTLEMENT_LAG_DAY_COUNT                   as                   FROI_TO_SETTLEMENT_LAG_DAY_COUNT
, FROI_TO_FIRST_IW_CONTACT_LAG_DAY_COUNT             as             FROI_TO_FIRST_IW_CONTACT_LAG_DAY_COUNT
, FROI_TO_FIRST_EMPLOYER_CONTACT_LAG_DAY_COUNT       as       FROI_TO_FIRST_EMPLOYER_CONTACT_LAG_DAY_COUNT
		FROM     LOGIC_CLM   ), 
RENAME_IW as ( SELECT 
  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY
, CUSTOMER_NUMBER                                    as                       IW_CORESUITE_CUSTOMER_NUMBER
, RECORD_EFFECTIVE_DATE                              as                           IW_RECORD_EFFECTIVE_DATE
, RECORD_END_DATE                                    as                                 IW_RECORD_END_DATE
, CURRENT_RECORD_IND                                 as                              IW_CURRENT_RECORD_IND
				FROM     LOGIC_IW   ), 
RENAME_P_ICD as ( SELECT 
  ICD_HKEY                                           as                                   PRIMARY_ICD_HKEY
, ICD_CODE                                           as                                     P_ICD_ICD_CODE
, RECORD_EFFECTIVE_DATE                              as                        P_ICD_RECORD_EFFECTIVE_DATE
, RECORD_END_DATE                                    as                              P_ICD_RECORD_END_DATE
, CURRENT_RECORD_IND                                 as                             ICD_CURRENT_RECORD_IND
				FROM     LOGIC_P_ICD   ), 
RENAME_EMP as ( SELECT 
  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY
, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER
, RECORD_EFFECTIVE_DATE                              as                          EMP_RECORD_EFFECTIVE_DATE
, RECORD_END_DATE                                    as                                EMP_RECORD_END_DATE
, CURRENT_RECORD_IND                                 as                             EMP_CURRENT_RECORD_IND
				FROM     LOGIC_EMP   ),
RENAME_MC as ( SELECT 
   MANUAL_CLASS_HKEY                                 as                             MANUAL_CLASS_CODE_HKEY
, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND
, MANUAL_CLASS_CODE                                  as                                  MANUAL_CLASS_CODE
, MANUAL_CLASS_SUFFIX_CODE                           as                           MANUAL_CLASS_SUFFIX_CODE
						FROM     LOGIC_MC   ), 
RENAME_INVEST as ( SELECT 
  ACP_START_DATE_KEY                                 as                                 ACP_START_DATE_KEY
, ACP_END_DATE_KEY                                   as                                   ACP_END_DATE_KEY
, PRIMARY_CSS_HKEY                                   as                                   PRIMARY_CSS_HKEY
, INVESTIGATION_HKEY                                 as                                 INVESTIGATION_HKEY
, ACP_VALIDATION_LAG_DAY_COUNT                       as                       ACP_VALIDATION_LAG_DAY_COUNT
, CLAIM_NUMBER                                       as                                INVEST_CLAIM_NUMBER
				FROM     LOGIC_INVEST   ), 
RENAME_CSPC as ( SELECT 
  FIRST_MEDICAL_PAYMENT_DATE                         as                         FIRST_MEDICAL_PAYMENT_DATE
, FIRST_MEDICAL_PAYMENT_DATE_KEY                     as                     FIRST_MEDICAL_PAYMENT_DATE_KEY
, LAST_MEDICAL_PAYMENT_DATE_KEY                      as                      LAST_MEDICAL_PAYMENT_DATE_KEY
, FIRST_INDEMNITY_PAYMENT_DATE                       as                       FIRST_INDEMNITY_PAYMENT_DATE
, FIRST_INDEMNITY_PAYMENT_DATE_KEY                   as                   FIRST_INDEMNITY_PAYMENT_DATE_KEY
, LAST_INDEMNITY_PAYMENT_DATE_KEY                    as                    LAST_INDEMNITY_PAYMENT_DATE_KEY
, PAYMENT_AMOUNTS_AS_OF_DATE_KEY                     as                     PAYMENT_AMOUNTS_AS_OF_DATE_KEY
, MEDICAL_HOSPITAL_AMOUNT                            as                            MEDICAL_HOSPITAL_AMOUNT
, MEDICAL_CLINIC_AMOUNT                              as                              MEDICAL_CLINIC_AMOUNT
, MEDICAL_DOCTOR_AMOUNT                              as                              MEDICAL_DOCTOR_AMOUNT
, MEDICAL_NURSING_SERVICES_AMOUNT                    as                    MEDICAL_NURSING_SERVICES_AMOUNT
, MEDICAL_DRUGS_PHARMACY_AMOUNT                      as                      MEDICAL_DRUGS_PHARMACY_AMOUNT
, MEDICAL_XRAY_RADIOLOGY_AMOUNT                      as                      MEDICAL_XRAY_RADIOLOGY_AMOUNT
, MEDICAL_LAB_PATHOLOGY_AMOUNT                       as                       MEDICAL_LAB_PATHOLOGY_AMOUNT
, MEDICAL_MISC_AMOUNT                                as                                MEDICAL_MISC_AMOUNT
, MEDICAL_OTHER_AMOUNT                               as                               MEDICAL_OTHER_AMOUNT
, MEDICAL_FUNERAL_AMOUNT                             as                             MEDICAL_FUNERAL_AMOUNT
, MEDICAL_COURT_AMOUNT                               as                               MEDICAL_COURT_AMOUNT
, TOTAL_MEDICAL_PAID_AMOUNT                          as                          TOTAL_MEDICAL_PAID_AMOUNT
, INDEMNITY_DWRF_AMOUNT                              as                              INDEMNITY_DWRF_AMOUNT
, INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT              as              INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT
, INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT               as               INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT
, INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT                  as                  INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT
, INDEMNITY_PTD_AMOUNT                               as                               INDEMNITY_PTD_AMOUNT
, INDEMNITY_TEMPORARY_TOTAL_AMOUNT                   as                   INDEMNITY_TEMPORARY_TOTAL_AMOUNT
, INDEMNITY_TEMPORARY_PARTIAL_AMOUNT                 as                 INDEMNITY_TEMPORARY_PARTIAL_AMOUNT
, INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT             as             INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT
, INDEMNITY_DEATH_BENEFIT_AMOUNT                     as                     INDEMNITY_DEATH_BENEFIT_AMOUNT
, INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT          as          INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT
, INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT      as      INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT
, INDEMNITY_OTHER_INDEMNITY_AMOUNT                   as                   INDEMNITY_OTHER_INDEMNITY_AMOUNT
, TOTAL_INDEMNITY_PAID_AMOUNT                        as                        TOTAL_INDEMNITY_PAID_AMOUNT
, TOTAL_CLAIM_PAYMENT_AMOUNT                         as                         TOTAL_CLAIM_PAYMENT_AMOUNT
, INDEMNITY_WORKING_WAGE_LOSS_AMOUNT                 as                 INDEMNITY_WORKING_WAGE_LOSS_AMOUNT
, CLAIM_NUMBER                                       as                                  CSPC_CLAIM_NUMBER
				FROM     LOGIC_CSPC   ),
RENAME_INV as ( SELECT 
  CLAIM_NUMBER                                       as                                   INV_CLAIM_NUMBER
, FIRST_MEDICAL_SERVICE_DATE_KEY                     as                     FIRST_MEDICAL_SERVICE_DATE_KEY
, LAST_MEDICAL_SERVICE_DATE_KEY                      as                      LAST_MEDICAL_SERVICE_DATE_KEY
				FROM     LOGIC_INV   ),								
RENAME_NTWK as ( SELECT 
  NETWORK_HKEY                                       as                                  MCO_ASSIGNED_HKEY
, NETWORK_NUMBER                                     as                                NTWK_NETWORK_NUMBER
, RECORD_EFFECTIVE_DATE                              as                         NTWK_RECORD_EFFECTIVE_DATE
, RECORD_END_DATE                                    as                               NTWK_RECORD_END_DATE
				FROM     LOGIC_NTWK   ), 
RENAME_DM as ( SELECT 
  CLAIM_NUMBER                                       as                                    DM_CLAIM_NUMBER
, TOTAL_MISSED_DAY_COUNT                             as                             TOTAL_MISSED_DAY_COUNT
, DISABILITY_TYPE_HKEY                               as                               DISABILITY_TYPE_HKEY
				FROM     LOGIC_DM   ),
RENAME_IPS        as ( SELECT 
  PERMANENT_PARTIAL_PERCENT                          as           CLAIM_TOTAL_PERMANENT_PARTIAL_PERCENTAGE
, INDEMNITY_BENEFIT_PLAN_WEEK_COUNT                  as                  INDEMNITY_BENEFIT_PLAN_WEEK_COUNT
, BENEFIT_TYPE_DESC                                  as                                  BENEFIT_TYPE_DESC
, IP_VOID_IND                                        as                                        IP_VOID_IND
, ISS_VOID_IND                                       as                                       ISS_VOID_IND
, ISD_VOID_IND                                       as                                       ISD_VOID_IND
, ISDA_VOID_IND                                      as                                      ISDA_VOID_IND
, AGRE_ID                                            as                                        IPS_AGRE_ID
, INDEMNITY_PLAN_ID                                  as                                  INDEMNITY_PLAN_ID
				FROM     LOGIC_IPS   )
---- FILTER LAYER (uses aliases) ----
,
FILTER_CLM                            as ( SELECT * from    RENAME_CLM   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
FILTER_P_ICD                          as ( SELECT * from    RENAME_P_ICD   ),
FILTER_EMP                            as ( SELECT * from    RENAME_EMP   ),
FILTER_INVEST                            as ( SELECT * from    RENAME_INVEST   ),
FILTER_CSPC                            as ( SELECT * from    RENAME_CSPC   ),
FILTER_MC                             as ( SELECT * from    RENAME_MC 
                                            WHERE CURRENT_RECORD_IND = 'Y'  ),
FILTER_INV                            as ( SELECT * from    RENAME_INV   ),
FILTER_NTWK                           as ( SELECT * from    RENAME_NTWK   ),
FILTER_DM                             as ( SELECT * from    RENAME_DM   ),
FILTER_IPS                            as ( SELECT * from    RENAME_IPS 
                                            WHERE BENEFIT_TYPE_DESC IN ( '%PP', 'NWWL','WWL','LMWL')
AND IP_VOID_IND = 'N'
AND NVL(ISS_VOID_IND, 'N') = 'N'
AND NVL(ISD_VOID_IND, 'N') = 'N'
AND NVL(ISDA_VOID_IND, 'N') = 'N'  ),

CALC_IPS as (
	SELECT IPS_AGRE_ID, 
		    SUM(CASE WHEN BENEFIT_TYPE_DESC = '%PP' THEN CLAIM_TOTAL_PERMANENT_PARTIAL_PERCENTAGE END)::NUMERIC(15,4) AS CLAIM_TOTAL_PERMANENT_PARTIAL_PERCENTAGE
	   , SUM(CASE WHEN BENEFIT_TYPE_DESC = 'NWWL' THEN INDEMNITY_BENEFIT_PLAN_WEEK_COUNT END)::NUMERIC(32,2) AS CLAIM_TOTAL_NON_WORKING_WAGE_LOSS_WEEKS
	   , SUM(CASE WHEN BENEFIT_TYPE_DESC = 'WWL' THEN INDEMNITY_BENEFIT_PLAN_WEEK_COUNT END)::NUMERIC(32,2) AS CLAIM_TOTAL_WORKING_WAGE_LOSS_WEEKS
       , SUM(CASE WHEN BENEFIT_TYPE_DESC = 'LMWL' THEN INDEMNITY_BENEFIT_PLAN_WEEK_COUNT END)::NUMERIC(32,2) AS CLAIM_TOTAL_LIVING_MAINTENANCE_WAGE_LOSS_WEEKS
	from FILTER_IPS
	GROUP BY IPS_AGRE_ID
)
---- JOIN LAYER ----
,
CLM as ( SELECT * 
				FROM  FILTER_CLM
				LEFT JOIN FILTER_INVEST ON  FILTER_CLM.CLAIM_NUMBER =  FILTER_INVEST.INVEST_CLAIM_NUMBER 
				LEFT JOIN FILTER_CSPC ON  FILTER_CLM.CLAIM_NUMBER =  FILTER_CSPC.CSPC_CLAIM_NUMBER 
				LEFT JOIN FILTER_INV ON  FILTER_CLM.CLAIM_NUMBER =  FILTER_INV.INV_CLAIM_NUMBER 
                LEFT JOIN FILTER_DM ON  FILTER_CLM.CLAIM_NUMBER =  FILTER_DM.DM_CLAIM_NUMBER 
				LEFT JOIN CALC_IPS ON  FILTER_CLM.CLM_AGRE_ID =  CALC_IPS.IPS_AGRE_ID 
				LEFT JOIN FILTER_IW ON  coalesce( FILTER_CLM.IW_CUSTOMER_NUMBER, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND CURRENT_DATE BETWEEN FILTER_IW.IW_RECORD_EFFECTIVE_DATE AND coalesce( FILTER_IW.IW_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_P_ICD ON  coalesce( FILTER_CLM.PRIMARY_ICD_CODE,  'UNK') =  FILTER_P_ICD.P_ICD_ICD_CODE AND CURRENT_DATE  BETWEEN FILTER_P_ICD.P_ICD_RECORD_EFFECTIVE_DATE AND coalesce( FILTER_P_ICD.P_ICD_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_EMP ON  coalesce( FILTER_CLM.EMP_CUSTOMER_NUMBER, '99999') =  FILTER_EMP.CUSTOMER_NUMBER AND CURRENT_DATE  BETWEEN FILTER_EMP.EMP_RECORD_EFFECTIVE_DATE AND coalesce( FILTER_EMP.EMP_RECORD_END_DATE, '2099-12-31')  
				LEFT JOIN FILTER_MC ON  coalesce( FILTER_CLM.CLM_MANUAL_CLASS_CODE, '99999') =  FILTER_MC.MANUAL_CLASS_CODE 
				AND COALESCE(FILTER_CLM.DRVD_MANUAL_CLASS_SUFFIX_CODE, '99999') = FILTER_MC.MANUAL_CLASS_SUFFIX_CODE  
                LEFT JOIN FILTER_NTWK ON  coalesce( FILTER_CLM.MCO_NO, '00000') =  FILTER_NTWK.NTWK_NETWORK_NUMBER  
				AND CURRENT_DATE BETWEEN NTWK_RECORD_EFFECTIVE_DATE AND coalesce( NTWK_RECORD_END_DATE, '2099-12-31') ),
-- ETL join layer to handle NDC & ICDs that are outside of date range MD5('-2222') 


ETL_SRT  AS (

select CLM.*,
 FILTER_IW1.INJURED_WORKER_HKEY AS FILTER_IW1_INJURED_WORKER_HKEY
,FILTER_P_ICD1.PRIMARY_ICD_HKEY AS FILTER_P_ICD1_PRIMARY_ICD_HKEY
,FILTER_EMP1.EMPLOYER_HKEY  AS FILTER_EMP1_EMPLOYER_HKEY 

  from CLM CLM
		LEFT JOIN FILTER_IW FILTER_IW1 ON  coalesce( CLM.IW_CUSTOMER_NUMBER, '-1') =  FILTER_IW1.IW_CORESUITE_CUSTOMER_NUMBER 
                  and FILTER_IW1.IW_CURRENT_RECORD_IND = 'Y' 
		LEFT JOIN FILTER_P_ICD FILTER_P_ICD1 ON  coalesce( CLM.PRIMARY_ICD_CODE,  'Z') =  FILTER_P_ICD1.P_ICD_ICD_CODE 
		          and FILTER_P_ICD1.ICD_CURRENT_RECORD_IND = 'Y' 
		LEFT JOIN FILTER_EMP FILTER_EMP1 ON  coalesce( CLM.EMP_CUSTOMER_NUMBER, 'Z') =  FILTER_EMP1.CUSTOMER_NUMBER 
		          and FILTER_EMP1.EMP_CURRENT_RECORD_IND = 'Y'

)

SELECT 
  CLAIM_NUMBER
, POLICY_ORIGINAL_EFFECTIVE_DATE_KEY  
, INITIAL_FILE_DATE_KEY 
, FILE_DATE_KEY
, OCCURRENCE_DATE_KEY
, CASE WHEN INJURED_WORKER_HKEY IS NOT NULL THEN INJURED_WORKER_HKEY
               WHEN FILTER_IW1_INJURED_WORKER_HKEY IS NOT NULL THEN MD5('-2222')
               ELSE MD5('-1111') END AS INJURED_WORKER_HKEY
,  CASE WHEN PRIMARY_ICD_HKEY IS NOT NULL THEN PRIMARY_ICD_HKEY
                WHEN FILTER_P_ICD1_PRIMARY_ICD_HKEY IS NOT NULL THEN MD5('-2222')
                ELSE MD5('-1111') END AS PRIMARY_ICD_HKEY
,  CASE WHEN EMPLOYER_HKEY IS NOT NULL THEN EMPLOYER_HKEY
                WHEN FILTER_EMP1_EMPLOYER_HKEY IS NOT NULL THEN MD5('-2222')
                ELSE MD5('-1111') END AS EMPLOYER_HKEY
, coalesce( MANUAL_CLASS_CODE_HKEY, MD5( '-1111' )) AS MANUAL_CLASS_CODE_HKEY
, INDUSTRY_GROUP_HKEY		
, md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
)) AS FILING_PARTY_HKEY 
, ENTRY_USER_HKEY
, SOURCE_SYSTEM_CREATE_DATE_KEY
, CLAIM_RELEASE_DATE_KEY
, FIRST_DETERMINATION_DATE_KEY		
, NVL (ACP_START_DATE_KEY,'-1') AS ACP_START_DATE_KEY
, NVL (ACP_END_DATE_KEY,'-1') AS ACP_END_DATE_KEY
, coalesce( PRIMARY_CSS_HKEY, MD5( '-1111' )) as PRIMARY_CSS_HKEY
, coalesce( DETERMINATION_USER_HKEY, MD5( '-1111' )) as DETERMINATION_USER_HKEY
, coalesce( ORGANIZATIONAL_UNIT_HKEY, MD5( '-1111' )) as ORGANIZATIONAL_UNIT_HKEY
, NVL (INVESTIGATION_HKEY, MD5('99999')) AS  INVESTIGATION_HKEY
, FIRST_ASSIGNMENT_DATE_KEY
, LAST_ASSIGNMENT_DATE_KEY
, COALESCE(FIRST_MEDICAL_PAYMENT_DATE_KEY, '-1')  AS  FIRST_MEDICAL_PAYMENT_DATE_KEY 
,  COALESCE(LAST_MEDICAL_PAYMENT_DATE_KEY, '-1') AS LAST_MEDICAL_PAYMENT_DATE_KEY 
, FIRST_POR_VISIT_DATE_KEY	
, COALESCE(FIRST_MEDICAL_SERVICE_DATE_KEY, '-1')  AS FIRST_MEDICAL_SERVICE_DATE_KEY
, COALESCE(LAST_MEDICAL_SERVICE_DATE_KEY, '-1') AS LAST_MEDICAL_SERVICE_DATE_KEY
,  COALESCE(FIRST_INDEMNITY_PAYMENT_DATE_KEY, '-1') AS FIRST_INDEMNITY_PAYMENT_DATE_KEY
,  COALESCE(LAST_INDEMNITY_PAYMENT_DATE_KEY, '-1') AS  LAST_INDEMNITY_PAYMENT_DATE_KEY
, COALESCE(PAYMENT_AMOUNTS_AS_OF_DATE_KEY, '-1')  AS PAYMENT_AMOUNTS_AS_OF_DATE_KEY
, coalesce( POLICY_STANDING_HKEY, MD5( '-1111' )) as POLICY_STANDING_HKEY
, coalesce( MCO_ASSIGNED_HKEY, MD5( '-1111' )) as MCO_ASSIGNED_HKEY
, LAST_DAY_AT_WORK_DATE_KEY
, ESTIMATED_RETURN_TO_WORK_DATE_KEY
, ACTUAL_RETURN_TO_WORK_DATE_KEY
, coalesce( CLAIM_TYPE_STATUS_HKEY, MD5( '99999' )) as CLAIM_TYPE_STATUS_HKEY
, CLAIM_DETAIL_HKEY
, coalesce( CLAIM_ACCIDENT_DESC_HKEY, MD5( '99999' )) as CLAIM_ACCIDENT_DESC_HKEY
, EMPLOYER_PAID_PROGRAM_EFFECTIVE_DATE_KEY
, EMPLOYER_PAID_PROGRAM_EXPIRATION_DATE_KEY
, coalesce( CATASTROPHIC_INJURY_TYPE_HKEY, MD5( '99999' )) as CATASTROPHIC_INJURY_TYPE_HKEY
, CATASTROPHIC_EFFECTIVE_DATE_KEY
, CATASTROPHIC_EXPIRATION_DATE_KEY
, EMPLOYER_PAID_PROGRAM_EFFECTIVE_ENTRY_DATE_KEY
, POLICY_NUMBER
, BUSINESS_SEQUENCE_NUMBER
, AGE_AT_OCCURRENCE
, ACP_VALIDATION_LAG_DAY_COUNT
, FILE_LAG_DAY_COUNT
, FROI_TO_FIRST_DATE_OF_DETERMINATION_LAG_DAY_COUNT
, FROI_TO_ARTW_LAG_DAY_COUNT
, FROI_TO_CLOSED_LAG_DAY_COUNT
, FROI_TO_SETTLEMENT_LAG_DAY_COUNT
,DATEDIFF(DAYS, CLAIM_FILE_DATE ,  NULLIF(least(NVL(FIRST_INDEMNITY_PAYMENT_DATE,'2099-12-31'), NVL(FIRST_MEDICAL_PAYMENT_DATE,'2099-12-31')),'2099-12-31' )) AS FROI_TO_FIRST_PAYMENT_LAG_DAY_COUNT
, DATEDIFF(DAYS, CLAIM_FILE_DATE, FIRST_INDEMNITY_PAYMENT_DATE)  AS FROI_TO_FIRST_INDEMNITY_LAG_DAY_COUNT
, DATEDIFF(DAYS, CLAIM_FILE_DATE, FIRST_MEDICAL_PAYMENT_DATE)  AS FROI_TO_FIRST_MEDICAL_PAYMENT_LAG_DAY_COUNT
,  FROI_TO_FIRST_IW_CONTACT_LAG_DAY_COUNT
,  FROI_TO_FIRST_EMPLOYER_CONTACT_LAG_DAY_COUNT
, 0 AS  FROI_TO_FIRST_PROVIDER_CONTACT_LAG_DAY_COUNT
, 0 AS FROI_TO_FIRST_POR_CONTACT_LAG_DAY_COUNT
, TOTAL_MISSED_DAY_COUNT
, coalesce( DISABILITY_TYPE_HKEY, MD5( '99999' )) as DISABILITY_TYPE_HKEY
, 1 as CLAIM_COUNT
, INVOICE_DISTINCT_COUNT
, INVOICE_LINE_DISTINCT_COUNT
, MEDICAL_HOSPITAL_AMOUNT
, MEDICAL_CLINIC_AMOUNT
, MEDICAL_DOCTOR_AMOUNT
, MEDICAL_NURSING_SERVICES_AMOUNT
, MEDICAL_DRUGS_PHARMACY_AMOUNT
, MEDICAL_XRAY_RADIOLOGY_AMOUNT
, MEDICAL_LAB_PATHOLOGY_AMOUNT
, MEDICAL_MISC_AMOUNT
, MEDICAL_OTHER_AMOUNT
, MEDICAL_FUNERAL_AMOUNT
, MEDICAL_COURT_AMOUNT
, TOTAL_MEDICAL_PAID_AMOUNT
, INDEMNITY_DWRF_AMOUNT
, INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT
, INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT
, INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT
, INDEMNITY_PTD_AMOUNT
, INDEMNITY_TEMPORARY_TOTAL_AMOUNT
, INDEMNITY_TEMPORARY_PARTIAL_AMOUNT
, INDEMNITY_SCHEDULE_LOSS_AMOUNT
, INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT
, INDEMNITY_DEATH_BENEFIT_AMOUNT
, INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT
, INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT
, INDEMNITY_OTHER_INDEMNITY_AMOUNT
, TOTAL_INDEMNITY_PAID_AMOUNT
, TOTAL_CLAIM_PAYMENT_AMOUNT
, INDEMNITY_WORKING_WAGE_LOSS_AMOUNT
, INDEMNITY_NON_WORKING_WAGE_LOSS_AMOUNT 
, CLAIM_TOTAL_PERMANENT_PARTIAL_PERCENTAGE
, CLAIM_TOTAL_NON_WORKING_WAGE_LOSS_WEEKS
, CLAIM_TOTAL_WORKING_WAGE_LOSS_WEEKS
, CLAIM_TOTAL_LIVING_MAINTENANCE_WAGE_LOSS_WEEKS 
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM  
		
from ETL_SRT