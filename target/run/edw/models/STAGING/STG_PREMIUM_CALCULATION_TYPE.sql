

      create or replace  table DEV_EDW.STAGING.STG_PREMIUM_CALCULATION_TYPE  as
      (---- SRC LAYER ----
WITH
SRC_PCT as ( SELECT *     from     DEV_VIEWS.BWC_FILES.PREMIUM_CALCULATION_TYPE ),
//SRC_PCT            as ( SELECT *     FROM     PREMIUM_CALCULATION_TYPE) ,

---- LOGIC LAYER ----


LOGIC_PCT as ( SELECT 
		  TRIM( PREMIUM_CALCULATION_TYPE_DESC )              as                      PREMIUM_CALCULATION_TYPE_DESC 
		FROM SRC_PCT
            )

---- RENAME LAYER ----
,

RENAME_PCT        as ( SELECT 
		  PREMIUM_CALCULATION_TYPE_DESC                      as                      PREMIUM_CALCULATION_TYPE_DESC 
				FROM     LOGIC_PCT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCT                            as ( SELECT * FROM    RENAME_PCT   ),

---- JOIN LAYER ----

 JOIN_PCT         as  ( SELECT * 
				FROM  FILTER_PCT )
 SELECT * FROM  JOIN_PCT
      );
    