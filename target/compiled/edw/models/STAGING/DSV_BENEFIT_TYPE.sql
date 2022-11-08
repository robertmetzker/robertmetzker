

---- SRC LAYER ----
WITH
SRC_BT as ( SELECT *     from     STAGING.DST_BENEFIT_TYPE ),
//SRC_BT as ( SELECT *     from     DST_BENEFIT_TYPE) ,

---- LOGIC LAYER ----

LOGIC_BT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, BNFT_TYP_CD                                        as                                        BNFT_TYP_CD 
		, JUR_TYP_CD                                         as                                         JUR_TYP_CD 
		, BNFT_RPT_TYP_NM                                    as                                    BNFT_RPT_TYP_NM 
		, BNFT_TYP_NM                                        as                                        BNFT_TYP_NM 
		, JUR_TYP_NM                                         as                                         JUR_TYP_NM 
		, BNFT_CTG_TYP_NM                                    as                                    BNFT_CTG_TYP_NM 
		from SRC_BT
            )

---- RENAME LAYER ----
,

RENAME_BT as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, BNFT_TYP_CD                                        as                                  BENEFIT_TYPE_CODE
		, JUR_TYP_CD                                         as                             JURISDICTION_TYPE_CODE
		, BNFT_RPT_TYP_NM                                    as                        BENEFIT_REPORTING_TYPE_DESC
		, BNFT_TYP_NM                                        as                                  BENEFIT_TYPE_DESC
		, JUR_TYP_NM                                         as                             JURISDICTION_TYPE_DESC
		, BNFT_CTG_TYP_NM                                    as                         BENEFIT_CATEGORY_TYPE_DESC 
				FROM     LOGIC_BT   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_BT                             as ( SELECT * from    RENAME_BT   ),

---- JOIN LAYER ----

 JOIN_BT  as  ( SELECT * 
				FROM  FILTER_BT )
 SELECT 
   UNIQUE_ID_KEY
, BENEFIT_TYPE_CODE
, JURISDICTION_TYPE_CODE
, BENEFIT_REPORTING_TYPE_DESC
, BENEFIT_TYPE_DESC
, JURISDICTION_TYPE_DESC
, BENEFIT_CATEGORY_TYPE_DESC
  FROM  JOIN_BT