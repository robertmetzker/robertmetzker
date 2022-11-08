

      create or replace  table DEV_EDW.STAGING.DST_PREMIUM_CALCULATION_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_PCT            as ( SELECT *     FROM     STAGING.STG_PREMIUM_CALCULATION_TYPE ),
SRC_PP            as ( SELECT *     FROM     STAGING.STG_PREMIUM_PERIOD ),
SRC_PA            as ( SELECT *     FROM     STAGING.STG_POLICY_AUDIT ),
//SRC_PCT            as ( SELECT *     FROM     STG_PREMIUM_CALCULATION_TYPE) ,
//SRC_PP            as ( SELECT *     FROM     STG_PREMIUM_PERIOD ),
//SRC_PA            as ( SELECT *     FROM     STG_POLICY_AUDIT ),

---- LOGIC LAYER ----

LOGIC_PCT as ( SELECT 
		  TRIM( PREMIUM_CALCULATION_TYPE_DESC )              as                        PREMIUM_CALCULATION_TYPE_DESC
		FROM SRC_PCT
            ),

LOGIC_PP as ( SELECT DISTINCT
		  TRIM( PLCY_PRD_ID )                                 as                                         PLCY_PRD_ID
		, TRIM( PREM_TYP_CD )                                 as                                         PREM_TYP_CD
		, TRIM( PREM_TYP_NM )                                 as                                         PREM_TYP_NM
		FROM SRC_PP
            ),

LOGIC_PA as ( SELECT 
		  TRIM( PLCY_PRD_ID )                                as                                          PLCY_PRD_ID
		, TRIM( PLCY_AUDT_TYP_CD )                           as                                     PLCY_AUDT_TYP_CD
		, TRIM( PLCY_AUDT_TYP_NM )                           as                                     PLCY_AUDT_TYP_NM
		FROM SRC_PA
            )

---- RENAME LAYER ----
,
RENAME_PCT        as ( SELECT 

		  PREMIUM_CALCULATION_TYPE_DESC                      as                        PREMIUM_CALCULATION_TYPE_DESC
				FROM     LOGIC_PCT   ),
RENAME_PP as ( SELECT 
		  PLCY_PRD_ID                                        as                                       PP_PLCY_PRD_ID
		, PREM_TYP_CD                                        as                                          PREM_TYP_CD
		, PREM_TYP_NM                                        as                                          PREM_TYP_NM
		FROM LOGIC_PP   ),
RENAME_PA as ( SELECT 
		  PLCY_PRD_ID                                        as                                       PA_PLCY_PRD_ID
		, PLCY_AUDT_TYP_CD                                   as                                     PLCY_AUDT_TYP_CD
		, PLCY_AUDT_TYP_NM                                   as                                     PLCY_AUDT_TYP_NM
		FROM LOGIC_PA    )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCT                            as ( SELECT * FROM    RENAME_PCT   ),
FILTER_PP                            as ( SELECT * FROM    RENAME_PP   ),
FILTER_PA                            as ( SELECT * FROM    RENAME_PA  ),

---- JOIN LAYER ----
 JOIN_PP         as  ( SELECT DISTINCT PREM_TYP_CD, PREM_TYP_NM, PLCY_AUDT_TYP_CD, PLCY_AUDT_TYP_NM
				FROM  FILTER_PP
                 LEFT JOIN FILTER_PA ON  FILTER_PP.PP_PLCY_PRD_ID = FILTER_PA.PA_PLCY_PRD_ID AND FILTER_PP.PREM_TYP_CD = 'A'),
                 
 JOIN_PCT         as  ( SELECT * 
				FROM  FILTER_PCT
                 FULL OUTER JOIN JOIN_PP ON 1 = 1 AND (PREMIUM_CALCULATION_TYPE_DESC in ('PAYROLL REPORT', 'ESTIMATED PAYROLL REPORT', 'MULTIPLE ADJUSTMENTS') and PREM_TYP_CD = 'R')
                                or (PREMIUM_CALCULATION_TYPE_DESC in ('FIELD AUDIT ASSIGNED', 'FIELD AUDIT COMPLETED', 'TRUE UP AUDIT ASSIGNED', 'TRUE UP AUDIT COMPLETED', 'MULTIPLE ADJUSTMENTS') and PREM_TYP_CD = 'A')
                                or (PREMIUM_CALCULATION_TYPE_DESC in ('ORIGINAL ESTIMATE', 'EXPOSURE ADJUSTMENT', 'BASE RATE ADJUSTMENT', 'PURE RATE ADJUSTMENT', 'MULTIPLE ADJUSTMENTS') and PREM_TYP_CD = 'E'))
                 
 -------  ETL LAYER  ---------
 ,
ETL AS( SELECT
      PREMIUM_CALCULATION_TYPE_DESC
    , IND.VALUE AS CURRENT_PREMIUM_CALCULATION_IND     
    , PREM_TYP_CD
    , PREM_TYP_NM
    , coalesce(PLCY_AUDT_TYP_CD,'N/A') as PLCY_AUDT_TYP_CD
    , coalesce(PLCY_AUDT_TYP_NM, 'N/A') as PLCY_AUDT_TYP_NM
   FROM JOIN_PCT
  ,LATERAL STRTOK_SPLIT_TO_TABLE('Y-N','-') as IND
    )

  SELECT 
  md5(cast(
    
    coalesce(cast(PREMIUM_CALCULATION_TYPE_DESC as 
    varchar
), '') || '-' || coalesce(cast(CURRENT_PREMIUM_CALCULATION_IND as 
    varchar
), '') || '-' || coalesce(cast(PREM_TYP_CD as 
    varchar
), '') || '-' || coalesce(cast(PLCY_AUDT_TYP_CD as 
    varchar
), '')

 as 
    varchar
))  as  UNIQUE_ID_KEY,
  * 
FROM ETL
      );
    