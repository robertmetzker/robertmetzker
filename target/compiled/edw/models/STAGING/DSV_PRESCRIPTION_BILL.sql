
----SRC LAYER----
WITH
SRC_PB as ( SELECT *     from      STAGING.DST_PRESCRIPTION_BILL )
//SRC_PB as ( SELECT *     from      DST_PRESCRIPTION_BILL)
----LOGIC LAYER----
,
LOGIC_PB as ( SELECT 
		UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
		TRIM(SERVICE_LEVEL) AS SERVICE_LEVEL,
		TRIM(PROF_SERVICE) AS PROF_SERVICE,
		TRIM(REASON_SERVICE) AS REASON_SERVICE,
		TRIM(RESULT_SERVICE) AS RESULT_SERVICE,
		TRIM(GENERIC_DRUG_IND) AS GENERIC_DRUG_IND,
		TRIM(CLARIFICATION) AS CLARIFICATION,
		TRIM(PLAN_CODE) AS PLAN_CODE,
		TRIM(ORIGINATION_FLAG) AS ORIGINATION_FLAG,
		TRIM(SPECIAL_PROGRAM) AS SPECIAL_PROGRAM,
		TRIM(PROVIDER_LOCK) AS PROVIDER_LOCK,
		REFILL_IND AS REFILL_IND,
		TRIM(PHARMACIST_SERVICE_DESC) AS PHARMACIST_SERVICE_DESC,
		TRIM(SERVICE_REASON_DESC) AS SERVICE_REASON_DESC,
		TRIM(SERVICE_RESULT_DESC) AS SERVICE_RESULT_DESC,
		LOS_REF_DSC AS LOS_REF_DSC,
		DAW_REF_DSC AS DAW_REF_DSC,
		PCL_REF_DSC AS PCL_REF_DSC 
				from SRC_PB
            )
----RENAME LAYER ----
,
RENAME_PB as ( SELECT UNIQUE_ID_KEY AS UNIQUE_ID_KEY,
			
			SERVICE_LEVEL AS SERVICE_LEVEL_CODE,
			
			PROF_SERVICE AS PHARMACIST_SERVICE_CODE,
			
			REASON_SERVICE AS SERVICE_REASON_CODE,
			
			RESULT_SERVICE AS SERVICE_RESULT_CODE,
			
			GENERIC_DRUG_IND AS SUBMITTED_DAW_CODE,
			
			CLARIFICATION AS SUBMISSION_CLARIFICATION_CODE,
			
			PLAN_CODE AS PBM_BENEFIT_PLAN_TYPE_DESC,
			
			ORIGINATION_FLAG AS PBM_ORIGINATION_TYPE_DESC,
			
			SPECIAL_PROGRAM AS PHARM_SPECIAL_PROGRAM_DESC,
			
			PROVIDER_LOCK AS PBM_LOCK_IN_IND,
			
			REFILL_IND AS DRUG_REFILL_IND,PHARMACIST_SERVICE_DESC AS PHARMACIST_SERVICE_DESC,SERVICE_REASON_DESC AS SERVICE_REASON_DESC,SERVICE_RESULT_DESC AS SERVICE_RESULT_DESC,
			
			LOS_REF_DSC AS SERVICE_LEVEL_DESC,
			
			DAW_REF_DSC AS SUBMITTED_DAW_DESC,
			
			PCL_REF_DSC AS SUBMISSION_CLARIFICATION_DESC 
			from      LOGIC_PB
        )
----FILTER LAYER(uses aliases)----
,

        FILTER_PB as ( SELECT  * 
			from     RENAME_PB 
            
        )
----JOIN LAYER----
,
 JOIN_PB as ( SELECT * 
			from  FILTER_PB )
 SELECT * FROM JOIN_PB