

---- SRC LAYER ----
WITH
SRC_PCT            as ( SELECT *     FROM     STAGING.DST_PREMIUM_CALCULATION_TYPE ),
//SRC_PCT            as ( SELECT *     FROM     DST_PREMIUM_CALCULATION_TYPE) ,

---- LOGIC LAYER ----


LOGIC_PCT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, PREMIUM_CALCULATION_TYPE_DESC                      as                      PREMIUM_CALCULATION_TYPE_DESC 
		, CURRENT_PREMIUM_CALCULATION_IND                    as                    CURRENT_PREMIUM_CALCULATION_IND 
		, PREM_TYP_CD                                        as                                        PREM_TYP_CD 
        , PREM_TYP_NM                                        as                                        PREM_TYP_NM 
        , PLCY_AUDT_TYP_CD                                   as                                   PLCY_AUDT_TYP_CD 
        , PLCY_AUDT_TYP_NM                                   as                                   PLCY_AUDT_TYP_NM 
		FROM SRC_PCT
            )

---- RENAME LAYER ----
,

RENAME_PCT        as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, PREMIUM_CALCULATION_TYPE_DESC                      as                      PREMIUM_CALCULATION_TYPE_DESC
		, CURRENT_PREMIUM_CALCULATION_IND                    as                    CURRENT_PREMIUM_CALCULATION_IND
		, PREM_TYP_CD                                        as                                 EXPOSURE_TYPE_CODE
        , PREM_TYP_NM                                        as                                 EXPOSURE_TYPE_DESC
        , PLCY_AUDT_TYP_CD                                   as                           EXPOSURE_AUDIT_TYPE_CODE
        , PLCY_AUDT_TYP_NM                                   as                           EXPOSURE_AUDIT_TYPE_DESC
				FROM     LOGIC_PCT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_PCT                            as ( SELECT * FROM    RENAME_PCT   ),

---- JOIN LAYER ----

 JOIN_PCT         as  ( SELECT * 
				FROM  FILTER_PCT )
 SELECT * FROM  JOIN_PCT