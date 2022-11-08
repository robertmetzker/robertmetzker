
  create or replace  view DEV_EDW.STAGING.DSV_CLAIM_PAYMENT_SUMMARY  as (
    

---- SRC LAYER ----
WITH
SRC_CDPC as ( SELECT *     from     STAGING.DST_CLAIM_DETAIL_PAYMENT_CODING ),
//SRC_CDPC as ( SELECT *     from     DST_CLAIM_DETAIL_PAYMENT_CODING) ,

---- LOGIC LAYER ----

LOGIC_CDPC as ( SELECT 
		  CLAIM_NO                                           as                                           CLAIM_NO 
		, WRNT_DATE                                          as                                          WRNT_DATE 
		, ETL_PROCESS_DATE                                   as                                   ETL_PROCESS_DATE 
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT 
		, PYMNT_CODE_CTGRY                                   as                                   PYMNT_CODE_CTGRY
		from SRC_CDPC
            )

---- RENAME LAYER ----
,

RENAME_CDPC as ( SELECT 
		  CLAIM_NO                                           as                                       CLAIM_NUMBER
		, WRNT_DATE                                          as                         FIRST_MEDICAL_PAYMENT_DATE
		, WRNT_DATE                                          as                          LAST_MEDICAL_PAYMENT_DATE
		, WRNT_DATE                                          as                       FIRST_INDEMNITY_PAYMENT_DATE
		, WRNT_DATE                                          as                        LAST_INDEMNITY_PAYMENT_DATE
		, ETL_PROCESS_DATE                                   as                         PAYMENT_AMOUNTS_AS_OF_DATE
		, PYMNT_CODE_AMT                                     as                         TOTAL_CLAIM_PAYMENT_AMOUNT
		, PYMNT_CODE_AMT                                     as                          TOTAL_MEDICAL_PAID_AMOUNT
		, PYMNT_CODE_AMT                                     as                            MEDICAL_HOSPITAL_AMOUNT
		, PYMNT_CODE_AMT                                     as                              MEDICAL_CLINIC_AMOUNT
		, PYMNT_CODE_AMT                                     as                              MEDICAL_DOCTOR_AMOUNT
		, PYMNT_CODE_AMT                                     as                    MEDICAL_NURSING_SERVICES_AMOUNT
		, PYMNT_CODE_AMT                                     as                      MEDICAL_DRUGS_PHARMACY_AMOUNT
		, PYMNT_CODE_AMT                                     as                      MEDICAL_XRAY_RADIOLOGY_AMOUNT
		, PYMNT_CODE_AMT                                     as                       MEDICAL_LAB_PATHOLOGY_AMOUNT
		, PYMNT_CODE_AMT                                     as                                MEDICAL_MISC_AMOUNT
		, PYMNT_CODE_AMT                                     as                               MEDICAL_OTHER_AMOUNT
		, PYMNT_CODE_AMT                                     as                             MEDICAL_FUNERAL_AMOUNT
		, PYMNT_CODE_AMT                                     as                               MEDICAL_COURT_AMOUNT
		, PYMNT_CODE_AMT                                     as                        TOTAL_INDEMNITY_PAID_AMOUNT
		, PYMNT_CODE_AMT                                     as                              INDEMNITY_DWRF_AMOUNT
		, PYMNT_CODE_AMT                                     as              INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT
		, PYMNT_CODE_AMT                                     as               INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT
		, PYMNT_CODE_AMT                                     as                  INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT
		, PYMNT_CODE_AMT                                     as                               INDEMNITY_PTD_AMOUNT
		, PYMNT_CODE_AMT                                     as                   INDEMNITY_TEMPORARY_TOTAL_AMOUNT
		, PYMNT_CODE_AMT                                     as                 INDEMNITY_TEMPORARY_PARTIAL_AMOUNT
		, PYMNT_CODE_AMT                                     as                 INDEMNITY_PERMANENT_PARTIAL_AMOUNT
		, PYMNT_CODE_AMT                                     as             INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT
		, PYMNT_CODE_AMT                                     as                     INDEMNITY_DEATH_BENEFIT_AMOUNT
		, PYMNT_CODE_AMT                                     as          INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT
		, PYMNT_CODE_AMT                                     as                 INDEMNITY_WORKING_WAGE_LOSS_AMOUNT
		, PYMNT_CODE_AMT                                     as      INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT
		, PYMNT_CODE_AMT                                     as                   INDEMNITY_OTHER_INDEMNITY_AMOUNT
		, PYMNT_CODE_AMT                                     as                                ATTORNEY_FEE_AMOUNT
		, PYMNT_CODE_AMT                                     as                             CONTRACT_DOCTOR_AMOUNT
		, PYMNT_CODE_AMT                                     as                                      UNDEFINED_AMT 
		, PYMNT_CODE_CTGRY                                   as                                   PYMNT_CODE_CTGRY
		, WRNT_DATE                                          as                                          WRNT_DATE 
		, ETL_PROCESS_DATE                                   as                                   ETL_PROCESS_DATE 
		, PYMNT_CODE_AMT                                     as                                     PYMNT_CODE_AMT 
						FROM     LOGIC_CDPC   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CDPC                           as ( SELECT * from    RENAME_CDPC   ),

---- JOIN LAYER ----

 JOIN_CDPC  as  ( SELECT 
  md5(cast(
    
    coalesce(cast(CLAIM_NUMBER as 
    varchar
), '')

 as 
    varchar
))  AS UNIQUE_ID_KEY
 , CLAIM_NUMBER AS CLAIM_NUMBER
 , MIN(DECODE(LEFT(PYMNT_CODE_CTGRY, 4), 'MDCL', WRNT_DATE, NULL )) AS FIRST_MEDICAL_PAYMENT_DATE
 , MAX(DECODE(LEFT(PYMNT_CODE_CTGRY, 4), 'MDCL', WRNT_DATE, NULL )) AS LAST_MEDICAL_PAYMENT_DATE
 , MIN(DECODE(LEFT(PYMNT_CODE_CTGRY, 4), 'INDM', WRNT_DATE, NULL )) AS FIRST_INDEMNITY_PAYMENT_DATE
 , MAX(DECODE(LEFT(PYMNT_CODE_CTGRY, 4), 'INDM', WRNT_DATE, NULL )) AS LAST_INDEMNITY_PAYMENT_DATE
 , MAX(ETL_PROCESS_DATE) AS PAYMENT_AMOUNTS_AS_OF_DATE
 , SUM(CASE WHEN PYMNT_CODE_CTGRY IN ('UNDEFINED', 'ATRNY_FEE_AMT', 'CNTRCT_DOC_AMT') 
 THEN 0 ELSE PYMNT_CODE_AMT END ) AS TOTAL_CLAIM_PAYMENT_AMOUNT
 , SUM(DECODE(LEFT(PYMNT_CODE_CTGRY, 4), 'MDCL', PYMNT_CODE_AMT, 0)) TOTAL_MEDICAL_PAID_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_HSP_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_HOSPITAL_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_CLN_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_CLINIC_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_DR_AMT', PYMNT_CODE_AMT, 0))  AS 	MEDICAL_DOCTOR_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_NRS_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_NURSING_SERVICES_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_DRG_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_DRUGS_PHARMACY_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_XRY_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_XRAY_RADIOLOGY_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_LAB_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_LAB_PATHOLOGY_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_MSC_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_MISC_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_OTH_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_OTHER_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_FNR_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_FUNERAL_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'MDCL_CRT_AMT', PYMNT_CODE_AMT, 0)) AS 	MEDICAL_COURT_AMOUNT
 , SUM(DECODE(LEFT(PYMNT_CODE_CTGRY, 4),  'INDM', PYMNT_CODE_AMT, 0)) AS 	TOTAL_INDEMNITY_PAID_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_DWR_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_DWRF_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_FCD_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_FACIAL_DISFIGUREMENT_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_LSP_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_LUMP_SUM_SETTLEMENT_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_LSA_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_LUMP_SUM_ADVANCE_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_PTD_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_PTD_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_TTP_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_TEMPORARY_TOTAL_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_TPP_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_TEMPORARY_PARTIAL_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_PPP_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_PERMANENT_PARTIAL_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_PCT_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_PCT_PERMANENT_PARTIAL_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_DBN_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_DEATH_BENEFIT_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_LMR_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_LIVING_MAINTENANCE_REHAB_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_WLT_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_WORKING_WAGE_LOSS_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_LMW_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_LIVING_MAINTENANCE_WAGE_LOSS_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'INDM_OIN_AMT', PYMNT_CODE_AMT, 0)) AS 	INDEMNITY_OTHER_INDEMNITY_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'ATRNY_FEE_AMT', PYMNT_CODE_AMT, 0)) AS 	ATTORNEY_FEE_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'CNTRCT_DOC_AMT', PYMNT_CODE_AMT, 0)) AS 	CONTRACT_DOCTOR_AMOUNT
 , SUM(DECODE(PYMNT_CODE_CTGRY,  'UNDEFINED', PYMNT_CODE_AMT, 0)) AS 	UNDEFINED_AMT
                 
				FROM  FILTER_CDPC
                group by 2 )
				
 SELECT * FROM  JOIN_CDPC
  );
