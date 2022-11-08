

      create or replace  table DEV_EDW.STAGING.DST_POLICY_PERIOD  as
      (---- SRC LAYER ----
WITH
SRC_PP as ( SELECT *     from     STAGING.STG_POLICY_PERIOD ),
SRC_PCE as ( SELECT *     from     STAGING.STG_POLICY_CONTROL_ELEMENT ),
SRC_D as ( SELECT *     from     DEV_VIEWS.BWC_FILES.DATE_DIM ),
//SRC_PP as ( SELECT *     from     STG_POLICY_PERIOD) ,
//SRC_PCE as ( SELECT *     from     STG_POLICY_CONTROL_ELEMENT) ,
//SRC_D as ( SELECT *     from     "DEV_SOURCE"."BWC_FILES"."DATE_DIM") ,

---- LOGIC LAYER ----
LOGIC_PP as ( SELECT 
          PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , AGRE_ID                                            as                                            AGRE_ID
        , PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		from SRC_PP
            ),
LOGIC_PCE as ( SELECT 
          PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , CTL_ELEM_SUB_TYP_CD                                as                                     PEC_POLICY_IND 
		, TRIM( CTL_ELEM_TYP_CD )                            as                                    CTL_ELEM_TYP_CD 
		, TRIM( CTL_ELEM_SUB_TYP_CD )                        as                                CTL_ELEM_SUB_TYP_CD 
		, TRIM( VOID_IND )                                   as                                           VOID_IND 
		from SRC_PCE
            ),
LOGIC_D as ( SELECT 
          YEAR_YYYY_NO                                       as                                        YEAR_NUMBER
		, FSCL_YEAR_BGNG_DATE                                as                             FISCAL_YEAR_BEGIN_DATE
        , FSCL_YEAR_ENDNG_DATE                               as                               FISCAL_YEAR_END_DATE
        , ACTUAL_DT                                          as                                        ACTUAL_DATE 
		from SRC_D
            )

---- RENAME LAYER ----
,
RENAME_PP as ( SELECT 
		  PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
        , AGRE_ID                                            as                                            AGRE_ID
        , PLCY_NO                                            as                                            PLCY_NO
        , PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
				FROM     LOGIC_PP   ), 
RENAME_PCE as ( SELECT 
          PLCY_PRD_ID                                        as                                    PCE_PLCY_PRD_ID
        , PEC_POLICY_IND                                     as                                     PEC_POLICY_IND
		, CTL_ELEM_TYP_CD                                    as                                    CTL_ELEM_TYP_CD
		, CTL_ELEM_SUB_TYP_CD                                as                                CTL_ELEM_SUB_TYP_CD
		, VOID_IND                                           as                                       PCE_VOID_IND 
				FROM     LOGIC_PCE   ), 
RENAME_D as ( SELECT
          YEAR_NUMBER                                        as                                        YEAR_NUMBER
		, FISCAL_YEAR_BEGIN_DATE                             as                             FISCAL_YEAR_BEGIN_DATE
        , FISCAL_YEAR_END_DATE                               as                               FISCAL_YEAR_END_DATE
        , ACTUAL_DATE                                        as                                        ACTUAL_DATE 
				FROM     LOGIC_D   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * from    RENAME_PP   ),
FILTER_PCE                            as ( SELECT * from    RENAME_PCE 
                                            WHERE CTL_ELEM_TYP_CD = 'PLCY_TYP' AND PCE_VOID_IND = 'N'  ),
FILTER_D                              as ( SELECT * from    RENAME_D   ),

---- JOIN LAYER ----

PP as ( SELECT * 
				FROM  FILTER_PP
				LEFT JOIN FILTER_PCE ON  FILTER_PP.PLCY_PRD_ID =  FILTER_PCE.PCE_PLCY_PRD_ID 
								LEFT JOIN FILTER_D ON  FILTER_PP.PLCY_PRD_EFF_DT =  FILTER_D.ACTUAL_DATE  )

----ETL LAYER----                                                            
,
ETL AS (SELECT 
	    	  PLCY_PRD_EFF_DT
		    , PLCY_PRD_END_DT
			, PLCY_PRD_EFF_DT || ' - ' || PLCY_PRD_END_DT AS POLICY_PERIOD_DESC
		    , CASE WHEN PEC_POLICY_IND = 'PEC' THEN 'Y' 
		           ELSE 'N'   END  AS PEC_POLICY_IND
	    	, CASE WHEN CTL_ELEM_SUB_TYP_CD = 'PEC' THEN DATE('01-01-' || YEAR_NUMBER, 'MM-DD-YYYY') 
		           ELSE FISCAL_YEAR_BEGIN_DATE END             as               REPORTING_YEAR_EFFECTIVE_DATE 
	    	, CASE WHEN CTL_ELEM_SUB_TYP_CD = 'PEC' THEN DATE('12-31-' || YEAR_NUMBER, 'MM-DD-YYYY') 
		           ELSE FISCAL_YEAR_END_DATE END               as                     REPORTING_YEAR_END_DATE 
            , CASE WHEN PLCY_PRD_EFF_DT = min(PLCY_PRD_EFF_DT) over (partition by PLCY_NO, AGRE_ID) THEN 'Y'
                   ELSE 'N' END AS NEW_POLICY_IND
			, CASE WHEN CTL_ELEM_SUB_TYP_CD = 'PEC' THEN 'PEC' ||' '|| YEAR(REPORTING_YEAR_EFFECTIVE_DATE)
                   ELSE YEAR(REPORTING_YEAR_EFFECTIVE_DATE) ||' - '|| YEAR(REPORTING_YEAR_END_DATE) END AS REPORTING_YEAR_DESC
from PP)

SELECT DISTINCT 
md5(cast(
    
    coalesce(cast(PLCY_PRD_EFF_DT as 
    varchar
), '') || '-' || coalesce(cast(PLCY_PRD_END_DT as 
    varchar
), '') || '-' || coalesce(cast(PEC_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(NEW_POLICY_IND as 
    varchar
), '') || '-' || coalesce(cast(POLICY_PERIOD_DESC as 
    varchar
), '')

 as 
    varchar
)) AS UNIQUE_ID_KEY
, *
FROM ETL
      );
    