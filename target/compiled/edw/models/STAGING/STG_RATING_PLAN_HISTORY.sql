---- SRC LAYER ----
WITH
SRC_PP             as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD ),
SRC_PPRE           as ( SELECT *     FROM     STAGING.STG_POLICY_PERIOD_RATING_ELEMENT ),
//SRC_PP             as ( SELECT *     FROM     STG_POLICY_PERIOD) ,
//SRC_PPRE           as ( SELECT *     FROM     STG_POLICY_PERIOD_RATING_ELEMENT) ,

---- LOGIC LAYER ----


LOGIC_PP as ( SELECT 
		  TRIM( PLCY_NO )                                    as                                            PLCY_NO 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT 
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
		FROM SRC_PP
            ),

LOGIC_PPRE as ( SELECT 
		  PPRE_EFF_DT                                        as                                        PPRE_EFF_DT 
		, PPRE_END_DT                                        as                                        PPRE_END_DT 
		, TRIM( RT_ELEM_TYP_CD )                             as                                     RT_ELEM_TYP_CD 
		, TRIM( RT_ELEM_TYP_NM )                             as                                     RT_ELEM_TYP_NM 
		, TRIM( EXPRN_MOD_TYP_CD )                           as                                   EXPRN_MOD_TYP_CD 
		, TRIM( EXPRN_MOD_TYP_NM )                           as                                   EXPRN_MOD_TYP_NM 
		, COALESCE(EXPRN_MOD_FCTR, PPRE_RT, 1)               as                                   RATING_PLAN_RATE                                                   
		, cast( AUDIT_USER_CREA_DTM as DATE )                as                          RATING_PLAN_EFFECTIVE_DATE
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM                                                 
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID 
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
		FROM SRC_PPRE
	    WHERE RT_ELEM_TYP_CD in ('GRPEXPRTDPRGM', 'GRPRETRO', 'RTSPINTRA', 'EMOD', 'CLS_RT_TIER')
		QUALIFY (ROW_NUMBER() OVER(PARTITION BY PLCY_PRD_ID, PPRE_EFF_DT,AUDIT_USER_CREA_DTM::DATE
                                              ORDER BY VOID_IND, AUDIT_USER_CREA_DTM DESC)) = 1
            ),
LOGIC_PPRE2 as ( SELECT 
		  PLCY_PRD_ID                                        as                                         PLCY_PRD_ID
		, TRIM( RT_ELEM_TYP_CD )                             as                                      RT_ELEM_TYP_CD 
		, cast( AUDIT_USER_CREA_DTM as DATE)                 as                          RATING_PLAN_EFFECTIVE_DATE 
		FROM SRC_PPRE
		WHERE RT_ELEM_TYP_CD in ('GRPEXPRTDPRGM', 'GRPRETRO', 'RTSPINTRA')
			QUALIFY (ROW_NUMBER() OVER(PARTITION BY PLCY_PRD_ID, PPRE_EFF_DT,AUDIT_USER_CREA_DTM::DATE
                                              ORDER BY VOID_IND, AUDIT_USER_CREA_DTM DESC)) = 1
            )
---- RENAME LAYER ----
,

RENAME_PP         as ( SELECT 
		  PLCY_NO                                            as                                            PLCY_NO
		, PLCY_PRD_ID                                        as                                        PLCY_PRD_ID
		, PLCY_PRD_EFF_DT                                    as                                    PLCY_PRD_EFF_DT
		, PLCY_PRD_END_DT                                    as                                    PLCY_PRD_END_DT 
				FROM     LOGIC_PP   ), 
RENAME_PPRE       as ( SELECT 
		  PPRE_EFF_DT                                        as                                        PPRE_EFF_DT
		, PPRE_END_DT                                        as                                        PPRE_END_DT
		, RT_ELEM_TYP_CD                                     as                              RATING_PLAN_TYPE_CODE
		, RT_ELEM_TYP_NM                                     as                              RATING_PLAN_TYPE_DESC
		, EXPRN_MOD_TYP_CD                                   as                                EXPRN_MOD_TYPE_CODE
		, EXPRN_MOD_TYP_NM                                   as                                EXPRN_MOD_TYPE_DESC
		, RATING_PLAN_RATE                                   as                                   RATING_PLAN_RATE
		, RATING_PLAN_EFFECTIVE_DATE                                as                         RATING_PLAN_EFFECTIVE_DATE
		, PLCY_PRD_ID                                        as                                   PPRE_PLCY_PRD_ID
		, AUDIT_USER_CREA_DTM                                as                                AUDIT_USER_CREA_DTM
		, AUDIT_USER_UPDT_DTM                                as                                AUDIT_USER_UPDT_DTM 
				FROM     LOGIC_PPRE   ),

RENAME_PPRE2 as ( SELECT 
		  PLCY_PRD_ID                                        as                                    PPRE2_PLCY_PRD_ID
		, RT_ELEM_TYP_CD                                     as                              PPRE2_RATING_PLAN_TYPE_CODE 
		, RATING_PLAN_EFFECTIVE_DATE                         as                             PPRE2_RATING_PLAN_EFFECTIVE_DATE 
		FROM LOGIC_PPRE2
            )				

---- FILTER LAYER (uses aliases) ----
,
FILTER_PP                             as ( SELECT * FROM    RENAME_PP   ),
FILTER_PPRE                           as ( SELECT * FROM    RENAME_PPRE 
                                              ),
FILTER_PPRE2                           as ( SELECT PPRE2_PLCY_PRD_ID, min(PPRE2_RATING_PLAN_EFFECTIVE_DATE ) as MIN_GRP_DT  FROM    RENAME_PPRE2 
                                            group by PPRE2_PLCY_PRD_ID ),

---- JOIN LAYER ----

PP as ( SELECT * , case when RATING_PLAN_TYPE_CODE = 'CLS_RT_TIER' AND PPRE_EFF_DT = lead(PPRE_EFF_DT) 
over (partition by PLCY_NO, PLCY_PRD_ID,PPRE_EFF_DT order by RATING_PLAN_TYPE_CODE, AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) 
AND AUDIT_USER_CREA_DTM = lead(AUDIT_USER_CREA_DTM) 
over (partition by PLCY_NO, PLCY_PRD_ID,PPRE_EFF_DT order by RATING_PLAN_TYPE_CODE, AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) 
THEN 'Y' ELSE 'N' END AS EXCLUDE_BASE,

min(date(AUDIT_USER_CREA_DTM)) 
over (partition by FILTER_PPRE.PPRE_PLCY_PRD_ID, FILTER_PPRE.RATING_PLAN_TYPE_CODE, FILTER_PPRE.EXPRN_MOD_TYPE_CODE, FILTER_PPRE.RATING_PLAN_RATE order by AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) as MIN_CRT_DT,

max(coalesce(date(AUDIT_USER_UPDT_DTM),'2999-12-31')) 
over (partition by FILTER_PPRE.PPRE_PLCY_PRD_ID, FILTER_PPRE.RATING_PLAN_TYPE_CODE, FILTER_PPRE.EXPRN_MOD_TYPE_CODE, RATING_PLAN_RATE order by AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) as MAX_UPDT_DT

				FROM  FILTER_PP
				LEFT JOIN FILTER_PPRE ON  FILTER_PP.PLCY_PRD_ID =  FILTER_PPRE.PPRE_PLCY_PRD_ID  
                LEFT JOIN FILTER_PPRE2 ON FILTER_PP.PLCY_PRD_ID =  FILTER_PPRE2.PPRE2_PLCY_PRD_ID  ),
AGG_PP AS (
 SELECT *, case when RATING_PLAN_TYPE_CODE = 'EMOD' 
    and AUDIT_USER_CREA_DTM >= MIN_GRP_DT
    then 'Y' else 'N' end as EXCLUDE_EMOD
FROM PP
),

---- ETL LAYER ------------------

ETL AS (SELECT * FROM AGG_PP
 where EXCLUDE_BASE = 'N' and EXCLUDE_EMOD = 'N' )

SELECT  
 PLCY_NO
,PLCY_PRD_ID
,PLCY_PRD_EFF_DT
,PLCY_PRD_END_DT
,PPRE_EFF_DT
,PPRE_END_DT
,RATING_PLAN_TYPE_CODE
,RATING_PLAN_TYPE_DESC
,EXPRN_MOD_TYPE_CODE
,EXPRN_MOD_TYPE_DESC
,RATING_PLAN_RATE
,RATING_PLAN_EFFECTIVE_DATE
, LEAD(DATE(AUDIT_USER_CREA_DTM)) OVER (PARTITION BY PLCY_NO, PLCY_PRD_ID ORDER BY AUDIT_USER_CREA_DTM, AUDIT_USER_UPDT_DTM) - 1 as RATING_PLAN_END_DATE
, CASE WHEN RATING_PLAN_END_DATE IS NULL THEN 'Y' ELSE 'N' END AS                  CURRENT_RATING_PLAN_IND
FROM ETL