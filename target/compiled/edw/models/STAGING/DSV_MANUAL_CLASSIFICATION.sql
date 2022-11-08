

---- SRC LAYER ----
WITH
SRC_CLASS as ( SELECT *     from     STAGING.DST_MANUAL_CLASSIFICATION ),
//SRC_CLASS as ( SELECT *     from     DST_MANUAL_CLASSIFICATION) ,

---- LOGIC LAYER ----

LOGIC_CLASS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY 
		, WC_CLS_CLS_CD                                      as                                      WC_CLS_CLS_CD 
		, WC_CLS_SUFX_CLS_SUFX                               as                               WC_CLS_SUFX_CLS_SUFX 
		, WC_CLS_SUFX_NM                                     as                                     WC_CLS_SUFX_NM 
		, WC_CLS_SUFX_DSCNT_IND                              as                              WC_CLS_SUFX_DSCNT_IND 
		, WC_CLS_SUFX_EFF_DT                                 as                                 WC_CLS_SUFX_EFF_DT 
		, WC_CLS_SUFX_END_DT                                 as                                 WC_CLS_SUFX_END_DT 
		, WC_CLS_RT_TIER_TYP_CD                              as                              WC_CLS_RT_TIER_TYP_CD 
		, WC_CLS_RT_TIER_EFF_DT                              as                              WC_CLS_RT_TIER_EFF_DT 
		, WC_CLS_RT_TIER_END_DT                              as                              WC_CLS_RT_TIER_END_DT 
		, WC_CLS_RT_TIER_RT                                  as                                  WC_CLS_RT_TIER_RT 
		, WC_CLS_RT_TIER_TYP_NM                              as                              WC_CLS_RT_TIER_TYP_NM 
		from SRC_CLASS
            )

---- RENAME LAYER ----
,

RENAME_CLASS as ( SELECT 
		  UNIQUE_ID_KEY                                      as                                      UNIQUE_ID_KEY
		, WC_CLS_CLS_CD                                      as                                  MANUAL_CLASS_CODE
		, WC_CLS_SUFX_CLS_SUFX                               as                           MANUAL_CLASS_SUFFIX_CODE
		, WC_CLS_SUFX_NM                                     as                                  MANUAL_CLASS_DESC
		, WC_CLS_SUFX_DSCNT_IND                              as                                SUFFIX_DISCOUNT_IND
		, WC_CLS_SUFX_EFF_DT                                 as                   MANUAL_CLASS_DESC_EFFECTIVE_DATE
		, WC_CLS_SUFX_END_DT                                 as                         MANUAL_CLASS_DESC_END_DATE
		, WC_CLS_RT_TIER_TYP_CD                              as                                RATE_TIER_TYPE_CODE
		, WC_CLS_RT_TIER_EFF_DT                              as                   MANUAL_CLASS_RATE_EFFECTIVE_DATE
		, WC_CLS_RT_TIER_END_DT                              as                         MANUAL_CLASS_RATE_END_DATE
		, WC_CLS_RT_TIER_RT                                  as                                  MANUAL_CLASS_RATE
		, WC_CLS_RT_TIER_TYP_NM                              as                                RATE_TIER_TYPE_DESC 
				FROM     LOGIC_CLASS   )

---- FILTER LAYER (uses aliases) ----
,
FILTER_CLASS                          as ( SELECT * from    RENAME_CLASS   ),

---- JOIN LAYER ----

 JOIN_CLASS  as  ( SELECT * 
				FROM  FILTER_CLASS )
 SELECT * FROM  JOIN_CLASS