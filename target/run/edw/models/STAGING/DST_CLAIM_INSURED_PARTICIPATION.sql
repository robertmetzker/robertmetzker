

      create or replace  table DEV_EDW.STAGING.DST_CLAIM_INSURED_PARTICIPATION  as
      (---- SRC LAYER ----
WITH
SRC_CP             as ( SELECT *     FROM     STAGING.STG_CLAIM_PARTICIPATION ),
SRC_CL             as ( SELECT *     FROM     STAGING.STG_CLAIM ),
SRC_PSD            as ( SELECT *     FROM     STAGING.STG_POLICY_SUMMARY_DETAIL ),
SRC_CC             as ( SELECT *     FROM     STAGING.STG_CLAIM_COVERAGE ),
//SRC_CP             as ( SELECT *     FROM     STG_CLAIM_PARTICIPATION) ,
//SRC_CL             as ( SELECT *     FROM     STG_CLAIM) ,
//SRC_PSD            as ( SELECT *     FROM     STG_POLICY_SUMMARY_DETAIL) ,
//SRC_CC             as ( SELECT *     FROM     STG_CLAIM_COVERAGE) ,

---- LOGIC LAYER ----

LOGIC_CP_EXCL AS (SELECT 
		  AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		, TRIM( CP_VOID_IND )                                as                                        CP_VOID_IND  
		, CP_CLM_PTCP_ID                                     as                                     CP_CLM_PTCP_ID 
		FROM SRC_CP
            ), 
LOGIC_CP as ( SELECT 
		  CLM_AGRE_ID                                        as                                        CLM_AGRE_ID 
		, TRIM( CLM_NO )                                     as                                             CLM_NO 
		, CUST_ID                                            as                                            CUST_ID 
		, CLM_PTCP_EFF_DT                                    as                                    CLM_PTCP_EFF_DT 
		, TRIM( CLM_PTCP_PRI_IND )                           as                                   CLM_PTCP_PRI_IND 
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM 
		, NVL(AUDIT_USER_UPDT_DTM, '12/31/2099')             as                                AUDIT_USER_UPDT_DTM 
		, TRIM( CP_VOID_IND )                                as                                        CP_VOID_IND 
		, TRIM( PTCP_TYP_CD )                                as                                        PTCP_TYP_CD 
		, CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID 
		, CP_CLM_PTCP_ID                                     as                                     CP_CLM_PTCP_ID 
		FROM SRC_CP
            ),

LOGIC_CL as ( SELECT 
		  CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE 
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                                AUDIT_USER_CREA_DTM 
		, AGRE_ID                                            as                                            AGRE_ID 
		FROM SRC_CL
            ),

LOGIC_PSD as ( SELECT 
		  TRIM( PLCY_SUM_DTL_PLCY_NO )                       as                               PLCY_SUM_DTL_PLCY_NO 
		, PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID 
		FROM SRC_PSD
            ),

LOGIC_CC as ( SELECT 
		  CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID 
		, PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID 
		FROM SRC_CC
            )

---- RENAME LAYER ----
,

RENAME_CP         as ( SELECT 
		  CLM_AGRE_ID                                        as                                        CLM_AGRE_ID
		, CLM_NO                                             as                                             CLM_NO
		, CUST_ID                                            as                                      INSRD_CUST_ID
		, CLM_PTCP_EFF_DT                                    as                                  CLM_PTCP_EFF_DATE
		, CLM_PTCP_PRI_IND                                   as                                   CLM_PTCP_PRI_IND
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM
	    , CP_VOID_IND                                        as                                        CP_VOID_IND
		, PTCP_TYP_CD                                        as                                        PTCP_TYP_CD
		, CLM_PTCP_INSRD_ID                                  as                                  CLM_PTCP_INSRD_ID 
		, CP_CLM_PTCP_ID                                     as                                     CP_CLM_PTCP_ID 
				FROM     LOGIC_CP   ), 
RENAME_CL         as ( SELECT 
		  CLM_OCCR_DATE                                      as                                      CLM_OCCR_DATE
		, AUDIT_USER_CREA_DTM                                as                                     CLM_ENTRY_DATE
		, AGRE_ID                                            as                                            AGRE_ID 
				FROM     LOGIC_CL   ), 
RENAME_PSD        as ( SELECT 
		  PLCY_SUM_DTL_PLCY_NO                               as                                        PSD_PLCY_NO
		, PLCY_SUM_DTL_ID                                    as                                PSD_PLCY_SUM_DTL_ID 
				FROM     LOGIC_PSD   ), 
RENAME_CC         as ( SELECT 
		  CLM_PTCP_INSRD_ID                                  as                               CC_CLM_PTCP_INSRD_ID
		, PLCY_SUM_DTL_ID                                    as                                    PLCY_SUM_DTL_ID 
				FROM     LOGIC_CC   ),
RENAME_CP_EXCL AS (SELECT 
		  AUDIT_USER_UPDT_DTM                                as                            EXCL_AUDIT_USER_UPDT_DTM 
		, CP_VOID_IND                                        as                                    EXCL_CP_VOID_IND  
		, CP_CLM_PTCP_ID                                     as                                 EXCL_CP_CLM_PTCP_ID 
		FROM LOGIC_CP_EXCL
            ) 

---- FILTER LAYER (uses aliases) ----
,
FILTER_CP                             as ( SELECT * FROM    RENAME_CP 
                                            WHERE PTCP_TYP_CD = 'INSRD'  ),
FILTER_CL                             as ( SELECT * FROM    RENAME_CL   ),
FILTER_CC                             as ( SELECT * FROM    RENAME_CC   ),
FILTER_PSD                            as ( SELECT * FROM    RENAME_PSD   ),
FILTER_CP_EXCL                        as ( SELECT * FROM    RENAME_CP_EXCL 
                                            WHERE EXCL_CP_VOID_IND = 'Y' AND EXCL_AUDIT_USER_UPDT_DTM IS NULL ),

---- JOIN LAYER ----

CC as ( SELECT * 
				FROM  FILTER_CC
				LEFT JOIN FILTER_PSD ON  FILTER_CC.PLCY_SUM_DTL_ID =  FILTER_PSD.PSD_PLCY_SUM_DTL_ID  ),
CP as ( SELECT * FROM  FILTER_CP
                LEFT JOIN FILTER_CP_EXCL ON FILTER_CP.CP_CLM_PTCP_ID = FILTER_CP_EXCL.EXCL_CP_CLM_PTCP_ID
				INNER JOIN FILTER_CL ON  FILTER_CP.CLM_AGRE_ID =  FILTER_CL.AGRE_ID AND FILTER_CP_EXCL.EXCL_CP_CLM_PTCP_ID IS NULL
				LEFT JOIN CC ON  FILTER_CP.CLM_PTCP_INSRD_ID = CC.CC_CLM_PTCP_INSRD_ID ),
                
-----------------------------STEP 1----------------------
ETL1 AS (SELECT 
CLM_AGRE_ID, CLM_NO, INSRD_CUST_ID, CLM_PTCP_EFF_DATE, CLM_OCCR_DATE AS CLM_OCCR_DATE, CLM_ENTRY_DATE, CLM_PTCP_PRI_IND, AUDIT_USER_CREA_DTM
, AUDIT_USER_UPDT_DTM, PSD_PLCY_NO, CP_VOID_IND FROM CP
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_NO, AUDIT_USER_CREA_DTM::DATE ORDER BY CP_VOID_IND, AUDIT_USER_UPDT_DTM DESC, AUDIT_USER_CREA_DTM DESC)=1   ) 

------------------------------STEP 2--------
,ETL2 AS( SELECT CLM_AGRE_ID, CLM_NO, INSRD_CUST_ID, CLM_PTCP_EFF_DATE, CLM_PTCP_PRI_IND,CLM_OCCR_DATE,CLM_ENTRY_DATE, 
         CASE WHEN (DATE(AUDIT_USER_CREA_DTM) < LAG(DATE(AUDIT_USER_UPDT_DTM),1)  OVER (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM)
                AND DATE(AUDIT_USER_UPDT_DTM) > LAG(DATE(AUDIT_USER_UPDT_DTM),1)  OVER (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM)
                AND DATE(AUDIT_USER_CREA_DTM) <> LAG(DATE(AUDIT_USER_CREA_DTM),1) OVER (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM))
                AND LAG( INSRD_CUST_ID,1,0) OVER (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) <> 1000042034
               THEN DATEADD(DAY,1,LAG(AUDIT_USER_UPDT_DTM,1) OVER (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM)) --+1::DATE
               ELSE AUDIT_USER_CREA_DTM END AS AUDIT_USER_CREA_DTM
---
, CASE WHEN INSRD_CUST_ID IN (1000042034, 800000)
THEN DATEADD(DAY,-1,LEAD (AUDIT_USER_CREA_DTM,1) OVER (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM))--1::DATE
ELSE AUDIT_USER_UPDT_DTM END AS AUDIT_USER_UPDT_DTM
, PSD_PLCY_NO, CP_VOID_IND
--- Conditional change event below is to idenPARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTMtify the merge order
, CONDITIONAL_CHANGE_EVENT (CLM_NO|| INSRD_CUST_ID|| NVL(PSD_PLCY_NO, '0')) OVER (PARTITION BY CLM_NO ORDER BY AUDIT_USER_CREA_DTM,AUDIT_USER_UPDT_DTM) AS CCE
FROM ETL1
 --WINDOW W1 AS (PARTITION BY  CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM)  
 )

------------STEP 3------------------ 

,ETL3 AS (SELECT 
 CLM_AGRE_ID, CLM_NO, INSRD_CUST_ID, CLM_OCCR_DATE, CLM_ENTRY_DATE
 , MIN(CLM_PTCP_EFF_DATE) CLM_PTCP_EFF_DATE
 , MAX(CLM_PTCP_PRI_IND) AS CLM_PTCP_PRI_IND
 , MIN(AUDIT_USER_CREA_DTM) AS AUDIT_USER_CREA_DTM, MAX(AUDIT_USER_UPDT_DTM) AS AUDIT_USER_UPDT_DTM
 , MAX(PSD_PLCY_NO) PSD_PLCY_NO, MIN (CP_VOID_IND) AS CP_VOID_IND
 FROM ETL2
 GROUP BY 1,2,3,4,5, CCE
) 
-------------STEP 4----------------------------

,ETL4 AS (SELECT CLM_AGRE_ID, CLM_NO, INSRD_CUST_ID, CLM_PTCP_EFF_DATE, CLM_OCCR_DATE, CLM_ENTRY_DATE, CLM_PTCP_PRI_IND, AUDIT_USER_CREA_DTM,PSD_PLCY_NO, CP_VOID_IND,
  --- the below changes to the end date is to resolve the Overlaps        
         CASE WHEN DATE(AUDIT_USER_UPDT_DTM) > LEAD(DATE(AUDIT_USER_CREA_DTM),1) OVER (PARTITION BY CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) 
         THEN DATEADD(DAY,-1,LEAD(DATE(AUDIT_USER_CREA_DTM),1) OVER (PARTITION BY CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM)) --1
         WHEN (AUDIT_USER_CREA_DTM IS NOT NULL  
         AND CLM_NO <> LEAD(CLM_NO,1,'0') OVER (PARTITION BY CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM)) THEN NULL 
         ELSE AUDIT_USER_UPDT_DTM END AS AUDIT_USER_UPDT_DTM, 
                  
--- the below Flag is to remove the Overlaps records when its placeholder(i.e. No Insureds Found)
          CASE WHEN (ROW_NUMBER() OVER(PARTITION BY CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) <> 1
          AND INSRD_CUST_ID IN (1000042034, 800000) AND DATE(AUDIT_USER_CREA_DTM) = DATE(AUDIT_USER_UPDT_DTM)) THEN 'Y' ELSE 'N' END AS OVRLP_FLG
FROM ETL3 
) 

---------------STEP 5------------------------
,ETL5 AS (SELECT 
          CLM_AGRE_ID, 
          CLM_NO, 
          INSRD_CUST_ID, 
          CLM_PTCP_EFF_DATE, 
          CLM_OCCR_DATE, 
          CLM_ENTRY_DATE, 
          CLM_PTCP_PRI_IND, 
          AUDIT_USER_CREA_DTM, 
          AUDIT_USER_UPDT_DTM, 
          PSD_PLCY_NO
--- resolve where current row end date is equal to next row begin date.
, CASE WHEN DATE(AUDIT_USER_CREA_DTM)= LAG(NVL(DATE(AUDIT_USER_UPDT_DTM), CURRENT_DATE)) OVER (PARTITION BY CLM_NO ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM )
AND DATE(AUDIT_USER_CREA_DTM) <> NVL(DATE(AUDIT_USER_UPDT_DTM), CURRENT_DATE) 
THEN DATE(AUDIT_USER_CREA_DTM)+1 ELSE DATE(AUDIT_USER_CREA_DTM) END AS CP_EFF_DT
, NVL(DATE(AUDIT_USER_UPDT_DTM), '12/31/2099') AS CP_END_DT 
FROM ETL4
WHERE OVRLP_FLG = 'N'
ORDER BY CLM_NO,CP_EFF_DT)

----------------- FINAL ETL LAYER ----

SELECT md5(cast(
    
    coalesce(cast(CLM_AGRE_ID as 
    varchar
), '') || '-' || coalesce(cast(CP_EFF_DT as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
,CLM_AGRE_ID
,CLM_NO
,INSRD_CUST_ID
,CLM_PTCP_EFF_DATE
,CLM_OCCR_DATE
,CLM_ENTRY_DATE
,CLM_PTCP_PRI_IND
,AUDIT_USER_CREA_DTM
,AUDIT_USER_UPDT_DTM
,PSD_PLCY_NO
,CP_EFF_DT AS CP_EFF_DATE
,CP_END_DT AS CP_END_DATE
FROM ETL5
      );
    