

      create or replace  table DEV_EDW.STAGING.STG_BENEFIT_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_BT as ( SELECT *     from      DEV_VIEWS.PCMP.BENEFIT_TYPE ),
SRC_BCT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_CATEGORY_TYPE ),
SRC_BRT as ( SELECT *     from     DEV_VIEWS.PCMP.BENEFIT_REPORTING_TYPE ),
SRC_JT as ( SELECT *     from      DEV_VIEWS.PCMP.JURISDICTION_TYPE ),
//SRC_BT as ( SELECT *     from     BENEFIT_TYPE) ,
//SRC_BCT as ( SELECT *     from     BENEFIT_CATEGORY_TYPE) ,
//SRC_BRT as ( SELECT *     from     BENEFIT_REPORTING_TYPE) ,
//SRC_JT as ( SELECT *     from     JURISDICTION_TYPE) ,
---- LOGIC LAYER ----

LOGIC_BT as ( SELECT 
		  upper( TRIM( BNFT_TYP_CD ) )                       as                                        BNFT_TYP_CD 
		, upper( TRIM( BNFT_TYP_NM ) )                       as                                        BNFT_TYP_NM 
		, upper( TRIM( BNFT_CTG_TYP_CD ) )                   as                                    BNFT_CTG_TYP_CD 
		from SRC_BT
            ),
LOGIC_BCT as ( SELECT 
		  upper( TRIM( BNFT_CTG_TYP_NM ) )                   as                                    BNFT_CTG_TYP_NM 
		, upper( TRIM( BNFT_CTG_TYP_CD ) )                   as                                    BNFT_CTG_TYP_CD 
		from SRC_BCT
            ),
LOGIC_BRT as ( SELECT DISTINCT
		  upper( TRIM( BNFT_RPT_TYP_CD ) )                   as                                    BNFT_RPT_TYP_CD 
		, upper( TRIM( BNFT_RPT_TYP_NM ) )                   as                                    BNFT_RPT_TYP_NM 
		, upper( TRIM( JUR_TYP_CD ) )                        as                                         JUR_TYP_CD 
		from SRC_BRT
            ),
LOGIC_JT as ( SELECT 
		  upper( TRIM( JUR_TYP_NM ) )                        as                                         JUR_TYP_NM 
		, upper( TRIM( JUR_TYP_CD ) )                        as                                         JUR_TYP_CD 
		from SRC_JT
            )

---- RENAME LAYER ----
,

RENAME_BT as ( SELECT 
		  BNFT_TYP_CD                                        as                                        BNFT_TYP_CD
		, BNFT_TYP_NM                                        as                                        BNFT_TYP_NM
		, BNFT_CTG_TYP_CD                                    as                                    BNFT_CTG_TYP_CD 
				FROM     LOGIC_BT   ), 
RENAME_BCT as ( SELECT 
		  BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM
		, BNFT_CTG_TYP_CD                                    as                                BCT_BNFT_CTG_TYP_CD 
				FROM     LOGIC_BCT   ), 
RENAME_BRT as ( SELECT 
		  BNFT_RPT_TYP_CD                                    as                                    BNFT_RPT_TYP_CD
		, BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD 
				FROM     LOGIC_BRT   ), 
RENAME_JT as ( SELECT 
		  JUR_TYP_NM                                         as                                         JUR_TYP_NM
		, JUR_TYP_CD                                         as                                      JT_JUR_TYP_CD 
				FROM     LOGIC_JT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_BT                             as ( SELECT * from    RENAME_BT   ),
FILTER_BCT                            as ( SELECT * from    RENAME_BCT   ),
FILTER_BRT                            as ( SELECT * from    RENAME_BRT   ),
FILTER_JT                             as ( SELECT * from    RENAME_JT   ),

---- JOIN LAYER ----

BRT as ( SELECT * 
				FROM  FILTER_BRT
				LEFT JOIN  FILTER_JT ON  FILTER_BRT.JUR_TYP_CD =  FILTER_JT.JT_JUR_TYP_CD  ),
BT as ( SELECT * 
				FROM  FILTER_BT
				FULL OUTER JOIN BRT  
						LEFT JOIN FILTER_BCT ON  
						 equal_null(FILTER_BT.BNFT_CTG_TYP_CD, FILTER_BCT.BCT_BNFT_CTG_TYP_CD))
SELECT 
		  BNFT_TYP_CD
		, BNFT_TYP_NM
		, BNFT_CTG_TYP_CD
		, BNFT_CTG_TYP_NM
		, BNFT_RPT_TYP_CD
		, BNFT_RPT_TYP_NM
		, JUR_TYP_CD
		, JUR_TYP_NM 
from BT
      );
    