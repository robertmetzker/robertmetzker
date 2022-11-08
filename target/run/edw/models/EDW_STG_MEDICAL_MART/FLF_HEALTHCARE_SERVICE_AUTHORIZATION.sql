

      create or replace  table DEV_EDW.EDW_STG_MEDICAL_MART.FLF_HEALTHCARE_SERVICE_AUTHORIZATION  as
      (

---- SRC LAYER ----
WITH
SRC_HSA as ( SELECT *     from     STAGING.DSV_HEALTHCARE_SERVICE_AUTHORIZATION ),
SRC_EMP as ( SELECT *     from     EDW_STAGING_DIM.DIM_EMPLOYER ),
SRC_IW as ( SELECT *     from     EDW_STAGING_DIM.DIM_INJURED_WORKER ),
//SRC_HSA as ( SELECT *     from     DSV_HEALTHCARE_SERVICE_AUTHORIZATION) ,
//SRC_EMP as ( SELECT *     from     DIM_EMPLOYER) ,
//SRC_IW as ( SELECT *     from     DIM_INJURED_WORKER) ,

---- LOGIC LAYER ----

LOGIC_HSA as ( SELECT 
		  CASE_NUMBER                                        as                                        CASE_NUMBER 
		, CASE_SERV_ID                                       as                                       CASE_SERV_ID 
		, SERVICE_TYPE_AUTHORIZATION_NUMBER                  as                  SERVICE_TYPE_AUTHORIZATION_NUMBER 
		, AUTHORIZATION_SERVICE_CODE_FROM                    as                    AUTHORIZATION_SERVICE_CODE_FROM 
		, AUTHORIZATION_SERVICE_CODE_THROUGH                 as                 AUTHORIZATION_SERVICE_CODE_THROUGH 
       , CASE WHEN  nullif(array_to_string(array_construct_compact( AUTHORIZATION_STATUS_CODE, AUTHORIZATION_SERVICE_TYPE_CODE, VOID_IND ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(AUTHORIZATION_STATUS_CODE as 
    varchar
), '') || '-' || coalesce(cast(AUTHORIZATION_SERVICE_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(VOID_IND as 
    varchar
), '')

 as 
    varchar
)) 
				END                                              as               HEALTHCARE_AUTHORIZATION_STATUS_HKEY 
		, AUTHORIZATION_DATE 								 as                                 AUTHORIZATION_DATE 
		,CASE WHEN AUTHORIZATION_DATE is NULL then -1
               WHEN replace(cast(AUTHORIZATION_DATE as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(AUTHORIZATION_DATE as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(AUTHORIZATION_DATE::varchar,'-','')::INTEGER
               END                                 as                                       AUTHORIZATION_DATE_KEY 
		, CASE WHEN AUTHORIZATION_FROM is NULL then -1
               WHEN replace(cast(AUTHORIZATION_FROM as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(AUTHORIZATION_FROM as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(AUTHORIZATION_FROM::varchar,'-','')::INTEGER
               END                                 as                                  AUTHORIZATION_FROM_DATE_KEY 
		, CASE WHEN AUTHORIZATION_THROUGH is NULL then -1
               WHEN replace(cast(AUTHORIZATION_THROUGH as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(AUTHORIZATION_THROUGH as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(AUTHORIZATION_THROUGH::varchar,'-','')::INTEGER
               END                                 as                               AUTHORIZATION_THROUGH_DATE_KEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( APP_CNTX_TYP_CD,CASE_CATEGORY_CODE,CASE_TYPE_CODE,CASE_PRTY_TYP_CD,CASE_RSOL_TYP_CD ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(APP_CNTX_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_CATEGORY_CODE as 
    varchar
), '') || '-' || coalesce(cast(CASE_TYPE_CODE as 
    varchar
), '') || '-' || coalesce(cast(CASE_PRTY_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_RSOL_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
				END                                          as                                     CASE_TYPE_HKEY 
        , CASE WHEN  nullif(array_to_string(array_construct_compact( CASE_STT_TYP_CD, CASE_STS_TYP_CD, CASE_STS_RSN_TYP_CD ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CASE_STT_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_STS_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(CASE_STS_RSN_TYP_CD as 
    varchar
), '')

 as 
    varchar
)) 
				END                                         as                                            CASE_STATUS_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( CLM_TYP_CD, CHNG_OVR_IND, CLM_STT_TYP_CD, CLM_STS_TYP_CD, CLM_TRANS_RSN_TYP_CD ),''),'') is NULL 
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
				END                                         as                                      CLAIM_TYPE_STATUS_HKEY 
		, CASE WHEN CASE_INITIATION_DATE is NULL then -1
               WHEN replace(cast(CASE_INITIATION_DATE as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(CASE_INITIATION_DATE as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(CASE_INITIATION_DATE::varchar,'-','')::INTEGER
               END                                 as                                     CASE_INITIATION_DATE_KEY 
		, CASE WHEN CASE_EFFECTIVE_DATE is NULL then -1
               WHEN replace(cast(CASE_EFFECTIVE_DATE as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(CASE_EFFECTIVE_DATE as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(CASE_EFFECTIVE_DATE::varchar,'-','')::INTEGER
               END                                 as                                      CASE_EFFECTIVE_DATE_KEY 
		, CASE WHEN CASE_DUE_DATE is NULL then -1
               WHEN replace(cast(CASE_DUE_DATE as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(CASE_DUE_DATE as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(CASE_DUE_DATE::varchar,'-','')::INTEGER
               END                                 as                                            CASE_DUE_DATE_KEY 
		, CASE WHEN AUTHORIZATION_THROUGH is NULL then -1
               WHEN replace(cast(AUTHORIZATION_THROUGH as varchar),'-','')::INTEGER < 19010101 then -2
               WHEN replace(cast(AUTHORIZATION_THROUGH as varchar),'-','')::INTEGER > 20991231 then -3
               ELSE replace(AUTHORIZATION_THROUGH::varchar,'-','')::INTEGER
               END                                 as                                     CASE_COMPLETION_DATE_KEY 
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
				END                                              as 		                          POLICY_STANDING_HKEY 
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
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
				END                                              as 		                         CLAIM_DETAIL_HKEY 
		, CASE WHEN  nullif(array_to_string(array_construct_compact( OCCR_PRMS_TYP_NM, CLM_OCCR_LOC_CNTRY_NM, CLM_OCCR_LOC_STT_CD, CLM_OCCR_LOC_STT_NM, CLM_OCCR_LOC_CNTY_NM, CLM_OCCR_LOC_CITY_NM, CLM_OCCR_LOC_POST_CD, CLM_OCCR_LOC_NM, CLM_OCCR_LOC_STR_1, CLM_OCCR_LOC_STR_2, CLM_OCCR_LOC_COMT ),''),'') is NULL 
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(OCCR_PRMS_TYP_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_CNTRY_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STT_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STT_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_CNTY_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_CITY_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_POST_CD as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_NM as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STR_1 as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_STR_2 as 
    varchar
), '') || '-' || coalesce(cast(CLM_OCCR_LOC_COMT as 
    varchar
), '')

 as 
    varchar
)) 
				END                                              as                             ACCIDENT_LOCATION_HKEY 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER 
		, PLCY_NO                                            as                                            PLCY_NO 
	  , AUTHORIZATION_SERVICE_TYPE_CODE                    as                    AUTHORIZATION_SERVICE_TYPE_CODE 
		, AUTHORIZATION_STATUS_CODE                          as                          AUTHORIZATION_STATUS_CODE 
		, CASE_TYPE_CODE                                     as                                     CASE_TYPE_CODE 
		, CASE_CATEGORY_CODE                                 as                                 CASE_CATEGORY_CODE 
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD 
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
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC 
		, CUST_NO                                            as                                            CUST_NO 
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO 
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO 
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE 
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD 
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD 
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND 
		from SRC_HSA
            ),
LOGIC_EMP as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_EMP
            ),

LOGIC_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    as                                    CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                              RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                    RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                                 CURRENT_RECORD_IND 
		from SRC_IW
            )
---- RENAME LAYER ----
,
RENAME_HSA        as ( SELECT 
		  CASE_NUMBER                                        as                                        CASE_NUMBER
		, CASE_SERV_ID                                       as                                    CASE_SERVICE_ID
		, SERVICE_TYPE_AUTHORIZATION_NUMBER                  as                  SERVICE_TYPE_AUTHORIZATION_NUMBER
		, AUTHORIZATION_SERVICE_CODE_FROM                    as                    AUTHORIZATION_SERVICE_CODE_FROM
		, AUTHORIZATION_SERVICE_CODE_THROUGH                 as                 AUTHORIZATION_SERVICE_CODE_THROUGH
    , HEALTHCARE_AUTHORIZATION_STATUS_HKEY               as               HEALTHCARE_AUTHORIZATION_STATUS_HKEY
		, AUTHORIZATION_DATE_KEY                             as                             AUTHORIZATION_DATE_KEY
		, AUTHORIZATION_DATE                                 as                                 AUTHORIZATION_DATE
		, AUTHORIZATION_FROM_DATE_KEY                        as                        AUTHORIZATION_FROM_DATE_KEY
		, AUTHORIZATION_THROUGH_DATE_KEY                     as                     AUTHORIZATION_THROUGH_DATE_KEY
		, CASE_TYPE_HKEY                                     as                                     CASE_TYPE_HKEY 
		, CASE_STATUS_HKEY                                   as                                   CASE_STATUS_HKEY 
		, CASE_INITIATION_DATE_KEY                           as                           CASE_INITIATION_DATE_KEY 
		, CASE_EFFECTIVE_DATE_KEY                            as                            CASE_EFFECTIVE_DATE_KEY 
		, CASE_DUE_DATE_KEY                                  as                                  CASE_DUE_DATE_KEY 
		, CASE_COMPLETION_DATE_KEY                           as                           CASE_COMPLETION_DATE_KEY 
		, POLICY_STANDING_HKEY                               as                               POLICY_STANDING_HKEY 
		, CLAIM_TYPE_STATUS_HKEY                             as                             CLAIM_TYPE_STATUS_HKEY 
		, CLM_LOSS_DESC                                      as                                      CLM_LOSS_DESC 
		, CLM_CLMT_JOB_TTL                                   as                                   CLM_CLMT_JOB_TTL 
		, CLAIM_DETAIL_HKEY                                  as                                  CLAIM_DETAIL_HKEY 
		, ACCIDENT_LOCATION_HKEY                             as                             ACCIDENT_LOCATION_HKEY 
		, CLAIM_NUMBER                                       as                                       CLAIM_NUMBER
		, PLCY_NO                                            as                                      POLICY_NUMBER
		, AUTHORIZATION_SERVICE_TYPE_CODE                    as                    AUTHORIZATION_SERVICE_TYPE_CODE
		, AUTHORIZATION_STATUS_CODE                          as                          AUTHORIZATION_STATUS_CODE
		, CASE_TYPE_CODE                                     as                                     CASE_TYPE_CODE
		, CLM_TYP_CD                                         as                                         CLM_TYP_CD
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
		, K_PROGRAM_REASON_DESC                              as                              K_PROGRAM_REASON_DESC
		, CUST_NO                                            as                                            CUST_NO
		, EMP_CUST_NO                                        as                                        EMP_CUST_NO
		, PLCY_NO                                            as                                            PLCY_NO
		, BUSN_SEQ_NO                                        as                                        BUSN_SEQ_NO
		, POLICY_TYPE_CODE                                   as                                   POLICY_TYPE_CODE
		, PLCY_STS_TYP_CD                                    as                                    PLCY_STS_TYP_CD
		, PLCY_STS_RSN_TYP_CD                                as                                PLCY_STS_RSN_TYP_CD
		, POLICY_ACTIVE_IND                                  as                                  POLICY_ACTIVE_IND
				FROM     LOGIC_HSA   ), 
RENAME_EMP as ( SELECT 
		  EMPLOYER_HKEY                                      as                                      EMPLOYER_HKEY 
		, CUSTOMER_NUMBER                                    as                                EMP_CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                          EMP_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                EMP_RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                             EMP_CURRENT_RECORD_IND 
				FROM     LOGIC_EMP   ), 
RENAME_IW as ( SELECT 
		  INJURED_WORKER_HKEY                                as                                INJURED_WORKER_HKEY 
		, CUSTOMER_NUMBER                                    as                       IW_CORESUITE_CUSTOMER_NUMBER 
		, RECORD_EFFECTIVE_DATE                              as                           IW_RECORD_EFFECTIVE_DATE 
		, RECORD_END_DATE                                    as                                 IW_RECORD_END_DATE 
		, CURRENT_RECORD_IND                                 as                              IW_CURRENT_RECORD_IND 
				FROM     LOGIC_IW   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_HSA                            as ( SELECT * from    RENAME_HSA   ),
FILTER_IW                             as ( SELECT * from    RENAME_IW   ),
FILTER_EMP                            as ( SELECT * from    RENAME_EMP   ),

---- JOIN LAYER ----

HSA as ( SELECT * 
				FROM  FILTER_HSA
				LEFT JOIN FILTER_IW ON  coalesce( FILTER_HSA.CUST_NO, '99999') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER 
				    AND FILTER_HSA.AUTHORIZATION_DATE BETWEEN IW_RECORD_EFFECTIVE_DATE AND coalesce( IW_RECORD_END_DATE, '2099-12-31') 
				LEFT JOIN FILTER_EMP ON  coalesce( FILTER_HSA.EMP_CUST_NO, '99999') =  FILTER_EMP.EMP_CUSTOMER_NUMBER 
						    AND FILTER_HSA.AUTHORIZATION_DATE  BETWEEN EMP_RECORD_EFFECTIVE_DATE AND coalesce( EMP_RECORD_END_DATE, '2099-12-31')  )

-- ETL join layer to handle that are outside of date range MD5('-2222') 								                                
, ETL_FLF AS (SELECT FLF_HSA.*
            	, FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AS FILTER_IW_CORESUITE_CUSTOMER_NUMBER
				, FILTER_EMP.EMP_CUSTOMER_NUMBER AS FILTER_EMP_EMP_CUSTOMER_NUMBER
              FROM HSA FLF_HSA
            	LEFT JOIN FILTER_IW ON   coalesce( FLF_HSA.CUST_NO,  '88888') =  FILTER_IW.IW_CORESUITE_CUSTOMER_NUMBER AND FILTER_IW.IW_CURRENT_RECORD_IND = 'Y'
				LEFT JOIN FILTER_EMP ON  coalesce( FLF_HSA.EMP_CUST_NO,  '88888') =  FILTER_EMP.EMP_CUSTOMER_NUMBER AND FILTER_EMP.EMP_CURRENT_RECORD_IND = 'Y' 
				) 
                

---- ETL LAYER -----------

SELECT 
  CASE_NUMBER
, CASE_SERVICE_ID
, SERVICE_TYPE_AUTHORIZATION_NUMBER 
, AUTHORIZATION_SERVICE_CODE_FROM
, AUTHORIZATION_SERVICE_CODE_THROUGH
, coalesce(HEALTHCARE_AUTHORIZATION_STATUS_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS HEALTHCARE_AUTHORIZATION_STATUS_HKEY
, AUTHORIZATION_DATE_KEY
, AUTHORIZATION_FROM_DATE_KEY
, AUTHORIZATION_THROUGH_DATE_KEY
, coalesce(CASE_TYPE_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS CASE_TYPE_HKEY
, coalesce(CASE_STATUS_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS CASE_STATUS_HKEY
, CASE_INITIATION_DATE_KEY
, CASE_EFFECTIVE_DATE_KEY
, CASE_DUE_DATE_KEY
, CASE_COMPLETION_DATE_KEY
, CASE WHEN EMPLOYER_HKEY IS NOT NULL THEN EMPLOYER_HKEY	
      WHEN FILTER_EMP_EMP_CUSTOMER_NUMBER IS NOT NULL THEN md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
)) 
      else md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
      END AS EMPLOYER_HKEY
, coalesce(POLICY_STANDING_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS POLICY_STANDING_HKEY
, CASE WHEN INJURED_WORKER_HKEY IS NOT NULL THEN INJURED_WORKER_HKEY	
      WHEN FILTER_IW_CORESUITE_CUSTOMER_NUMBER IS NOT NULL THEN md5(cast(
    
    coalesce(cast(-2222 as 
    varchar
), '')

 as 
    varchar
)) 
      else md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))
      END AS INJURED_WORKER_HKEY
, CASE WHEN nullif(array_to_string(array_construct_compact( CLM_LOSS_DESC, CLM_CLMT_JOB_TTL),''),'') is NULL
			then MD5( '99999' ) ELSE md5(cast(
    
    coalesce(cast(CLM_LOSS_DESC as 
    varchar
), '') || '-' || coalesce(cast(CLM_CLMT_JOB_TTL as 
    varchar
), '')

 as 
    varchar
)) 
				END as CLAIM_ACCIDENT_DESC_HKEY 
, coalesce(CLAIM_TYPE_STATUS_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS CLAIM_TYPE_STATUS_HKEY
, CLAIM_DETAIL_HKEY
, coalesce(ACCIDENT_LOCATION_HKEY,md5(cast(
    
    coalesce(cast(99999 as 
    varchar
), '')

 as 
    varchar
))) AS ACCIDENT_LOCATION_HKEY
, CLAIM_NUMBER
, POLICY_NUMBER
, CURRENT_TIMESTAMP AS LOAD_DATETIME
, TRY_TO_TIMESTAMP('Invalid') AS UPDATE_DATETIME 
, 'CORESUITE' AS PRIMARY_SOURCE_SYSTEM 
FROM  ETL_FLF
      );
    